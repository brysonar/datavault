package org.datavaultplatform.worker.tasks;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.datavaultplatform.common.event.Error;
import org.datavaultplatform.common.event.Event;
import org.datavaultplatform.common.event.EventStream;
import org.datavaultplatform.common.event.InitStates;
import org.datavaultplatform.common.event.UpdateProgress;
import org.datavaultplatform.common.event.deposit.Complete;
import org.datavaultplatform.common.event.deposit.ComputedDigest;
import org.datavaultplatform.common.event.deposit.ComputedSize;
import org.datavaultplatform.common.event.deposit.PackageComplete;
import org.datavaultplatform.common.event.deposit.Start;
import org.datavaultplatform.common.event.deposit.TransferComplete;
import org.datavaultplatform.common.io.Progress;
import org.datavaultplatform.common.model.FileStore;
import org.datavaultplatform.common.storage.ArchiveStore;
import org.datavaultplatform.common.storage.CheckSumEnum;
import org.datavaultplatform.common.storage.Device;
import org.datavaultplatform.common.storage.UserStore;
import org.datavaultplatform.common.storage.Verify;
import org.datavaultplatform.common.task.Context;
import org.datavaultplatform.common.task.Task;
import org.datavaultplatform.worker.exception.DepositException;
import org.datavaultplatform.worker.model.DepositDetails;
import org.datavaultplatform.worker.model.MetaData;
import org.datavaultplatform.worker.model.Storage;
import org.datavaultplatform.worker.model.UserStorage;
import org.datavaultplatform.worker.operations.IPackager;
import org.datavaultplatform.worker.operations.Identifier;
import org.datavaultplatform.worker.operations.ProgressTracker;
import org.datavaultplatform.worker.operations.Tar;
import org.datavaultplatform.worker.util.DataVaultConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Deposit implements ITaskAction {

	private final static Logger logger = LoggerFactory.getLogger(Deposit.class);

	private final IPackager packager;
	private final EventStream eventStream;

	@Autowired
	public Deposit(IPackager packager, EventStream eventStream) {
		super();
		this.packager = packager;
		this.eventStream = eventStream;
	}

	@Override
	public void performAction(Context context, Task task) {

		logger.info("Deposit job - performAction()");

		DepositDetails depositDetails = new DepositDetails().build(task);

		try {

			doAction(context, task, depositDetails);

		} catch (DepositException | RuntimeException e) {

			String errMsg = null;
			if (e instanceof DepositException) {
				errMsg = e.getMessage();
			} else {
				errMsg = "Deposit failed: " + e.getMessage();
			}

			logger.error(errMsg);
			Event event = new Error(depositDetails.getJobId(), depositDetails.getDepositId(), errMsg)
					.withUserId(depositDetails.getUserId());
			eventStream.send(event);
		}
	}

	private void doAction(Context context, Task task, DepositDetails depositDetails) throws DepositException {

		processRedeliver(depositDetails.isRedeliver());
		MetaData metaData = new MetaData().build(task.getProperties());

		ArrayList<String> states = buildStates();

		eventStream.send(new InitStates(depositDetails.getJobId(), depositDetails.getDepositId(), states)
				.withUserId(depositDetails.getUserId()));
		eventStream.send(new Start(depositDetails.getJobId(), depositDetails.getDepositId())
				.withUserId(depositDetails.getUserId()).withNextState(0));

		logger.info("Bag Id: {}, Deposit file:: {}", depositDetails.getBagId(), depositDetails.getFilePath());

		Storage storage = buildStorageConnections(task, depositDetails);

		checkUserStoreExists(storage.getUserStorage(), depositDetails);

		Path tempBagPath = createTempBagDirectory(context.getTempDir(), depositDetails);

		// Copy the target file to the bag directory
		eventStream.send(new UpdateProgress(depositDetails.getJobId(), depositDetails.getDepositId())
				.withUserId(depositDetails.getUserId()).withNextState(1));

		Path tempBagDataPath = createTempBagDataDirectory(tempBagPath);

		copyFromUserStorageToTempDataDirectory(storage.getUserStorage(), depositDetails, tempBagDataPath);

		// Bag the directory in-place
		eventStream.send(new TransferComplete(depositDetails.getJobId(), depositDetails.getDepositId())
				.withUserId(depositDetails.getUserId()).withNextState(2));

		packager.createBag(tempBagPath, tempBagDataPath);

		buildMetaData(metaData, tempBagPath, tempBagDataPath);

		File tarFile = generateTar(context.getTempDir(), depositDetails.getBagId(), tempBagPath);
		String tarHash = Verify.getDigest(tarFile);
		logger.info("Checksum: " + tarHash);
		CheckSumEnum tarHashAlgorithm = Verify.getAlgorithmCheckSum();
		logger.info("Checksum algorithm: " + tarHashAlgorithm.getJavaSecurityAlgorithm());

		long archiveSize = tarFile.length();
		logger.info("Tar file: " + archiveSize + " bytes");

		packager.addTarfileChecksum(tempBagPath, tarFile.toPath(), tarHash, tarHashAlgorithm);
		
		eventStream.send(new PackageComplete(depositDetails.getJobId(), depositDetails.getDepositId())
				.withUserId(depositDetails.getUserId()).withNextState(3));

		eventStream.send(
				new ComputedDigest(depositDetails.getJobId(), depositDetails.getDepositId(), tarHash, tarHashAlgorithm.getJavaSecurityAlgorithm())
						.withUserId(depositDetails.getUserId()));

		copyToMetaDirectory(context, depositDetails, tempBagPath.toFile());

		String archiveId = copyTarToArchiveStorage(storage.getArchiveFs(), depositDetails, tarFile);

		// TODO there is multiple deletes of this directory - see verify archive
		//if (DataVaultConstants.doTempDirectoryCleanUp) {
			deleteDirectory(tempBagDataPath.toFile());
		//}

		eventStream.send(new UpdateProgress(depositDetails.getJobId(), depositDetails.getDepositId())
				.withUserId(depositDetails.getUserId()).withNextState(4));

		verifyArchive(context, storage, tarFile, tarHash, archiveId);

		eventStream.send(new Complete(depositDetails.getJobId(), depositDetails.getDepositId(), archiveId, archiveSize)
				.withUserId(depositDetails.getUserId()).withNextState(5));

		logger.info("Deposit complete: {}", archiveId);

	}



	private File generateTar(Path tempDirectory, String bagId, Path tempBagPath) {
		// Tar the bag directory
		logger.info("Creating tar file ...");
		String tarFileName = bagId + ".tar";
		File tarFile = tempDirectory.resolve(tarFileName).toFile();
		logger.debug("Creating tar file: {}", tarFile.getAbsolutePath());

		Tar.createTar(tempBagPath.toFile(), tarFile);
		return tarFile;
	}

	private void buildMetaData(MetaData metaData, Path tempBagPath, Path tempBagDataPath) {
		// Identify the deposit file types
		logger.info("Identifying file types ...");
		String fileTypeMetadata = buildFileTypeMetaData(tempBagDataPath);

		// Add vault/deposit/type metadata to the bag
		packager.addMetadata(tempBagPath.toFile(), metaData.getDepositMetadata(), metaData.getVaultMetadata(),
				fileTypeMetadata, metaData.getExternalMetadata());
	}

	private String buildFileTypeMetaData(Path tempBagDataPath) {
		HashMap<String, String> fileTypes = Identifier.detectDirectory(tempBagDataPath);
		ObjectMapper mapper = new ObjectMapper();
		String fileTypeMetadata;
		try {
			fileTypeMetadata = mapper.writeValueAsString(fileTypes);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
		return fileTypeMetadata;
	}

	private Path createTempBagDataDirectory(Path tempBagPath) {
		Path tempBagDataPath = tempBagPath.resolve(DataVaultConstants.DATA);
		tempBagDataPath.toFile().mkdir();
		return tempBagDataPath;
	}

	private Path createTempBagDirectory(Path tempDirectory, DepositDetails depositDetails) {
		// Create a new directory based on the broker-generated UUID
		Path tempBagPath = tempDirectory.resolve(depositDetails.getBagId());
		// bagPath = bagPath.resolve("data");

		File tempBagDir = tempBagPath.toFile();
		logger.debug("Bag Directory: {}", tempBagDir.getAbsolutePath());
		boolean isBagDirectoryCreatedSucessfully = tempBagDir.mkdir();

		if (!isBagDirectoryCreatedSucessfully) {
			throw new RuntimeException("Bag directory not created succesfully");
		}
		return tempBagPath;
	}

	private void copyToMetaDirectory(Context context, DepositDetails depositDetails, File tempBagDir) {
		// Create the meta directory for the bag information
		Path metaPath = context.getMetaDir().resolve(depositDetails.getBagId());
		logger.info("create meta data directory: {}", metaPath.toString());
		File metaDir = metaPath.toFile();
		metaDir.mkdir();
		// TODO throw exception if directory not created successfully

		// Copy bag meta files to the meta directory
		logger.info("Copying meta files ...");
		packager.extractMetadata(tempBagDir, metaDir);
	}

	private void verifyArchive(Context context, Storage storage, File tarFile, String tarHash, String archiveId) {
		logger.info("Verifying archive package ...");

		ArchiveStore archiveFs = storage.getArchiveFs();

		logger.info("Verification method: " + archiveFs.getVerifyMethod());

		if (archiveFs.getVerifyMethod() == Verify.Method.LOCAL_ONLY) {

			// Verify the contents of the temporary file
			verifyTarFile(context.getTempDir(), tarFile, null);

		} else if (archiveFs.getVerifyMethod() == Verify.Method.COPY_BACK) {

			// Delete the existing temporary file
			if (tarFile.exists()) {
				logger.debug("About to delete tar file: {}", tarFile.getAbsolutePath());
				tarFile.delete();
			}

			// Copy file back from the archive storage
			copyBackFromArchive(storage.getArchiveFs(), archiveId, tarFile);

			// Verify the contents
			verifyTarFile(context.getTempDir(), tarFile, tarHash);
		}
	}



	private void checkUserStoreExists(UserStorage userStorage, DepositDetails depositDetails) throws DepositException {

		if (!userStorage.getUserStore().exists(depositDetails.getFilePath())) {
			String errMsg = "Deposit failed: file not found";
			throw new DepositException(errMsg);
		}
	}

	private Storage buildStorageConnections(Task task, DepositDetails depositDetails) throws DepositException {
		UserStorage userStorage = connectToUserStorage(task.getUserFileStore(), depositDetails);
		ArchiveStore archiveFs = connectToArchiveStorage(task.getArchiveFileStore(), depositDetails);
		Storage storage = new Storage(userStorage, archiveFs);
		return storage;
	}
	
	private UserStorage connectToUserStorage(FileStore userFileStore, DepositDetails depositDetails)
			throws DepositException {
		// Connect to the user storage
		try {
			Class<?> clazz = Class.forName(userFileStore.getStorageClass());
			Constructor<?> constructor = clazz.getConstructor(String.class, Map.class);
			Object instance = constructor.newInstance(userFileStore.getStorageClass(), userFileStore.getProperties());

			Device userFs = (Device) instance;
			UserStore userStore = (UserStore) userFs;
			return new UserStorage(userFs, userStore);

		} catch (Exception e) {
			String errMsg = "Deposit failed: could not access user filesystem";
			throw new DepositException(errMsg, e);
		}
	}
	
	private ArchiveStore connectToArchiveStorage(org.datavaultplatform.common.model.ArchiveStore archiveFileStore,
			DepositDetails depositDetails) throws DepositException {
		// Connect to the archive storage
		try {
			Class<?> clazz = Class.forName(archiveFileStore.getStorageClass());
			Constructor<?> constructor = clazz.getConstructor(String.class, Map.class);
			Object instance = constructor.newInstance(archiveFileStore.getStorageClass(),
					archiveFileStore.getProperties());
			return (ArchiveStore) instance;
		} catch (Exception e) {
			String errMsg = "Deposit failed: could not access archive filesystem";
			throw new DepositException(errMsg);
		}
	}



	private void processRedeliver(boolean isRedeliver) throws DepositException {
		if (isRedeliver) {
			String errMsg = "Deposit stopped: the message had been redelivered, please investigate";
			throw new DepositException(errMsg);
		}
	}

	private ArrayList<String> buildStates() {
		ArrayList<String> states = new ArrayList<>();
		states.add("Calculating size"); // 0
		states.add("Transferring"); // 1
		states.add("Packaging"); // 2
		states.add("Storing in archive"); // 3
		states.add("Verifying"); // 4
		states.add("Complete"); // 5
		return states;
	}

	private void copyFromUserStorageToTempDataDirectory(UserStorage userStorage, DepositDetails depositDetails,
			Path bagDataPath) {

		logger.info("Copying target from: {}, to bag directory: {}", depositDetails.getFilePath(), bagDataPath);
		try {
			String fileName = userStorage.getUserStore().getName(depositDetails.getFilePath());
			File outputFile = bagDataPath.resolve(fileName).toFile();

			// Compute bytes to copy
			long expectedBytes = userStorage.getUserStore().getSize(depositDetails.getFilePath());
			eventStream.send(new ComputedSize(depositDetails.getJobId(), depositDetails.getDepositId(), expectedBytes)
					.withUserId(depositDetails.getUserId()));

			// Display progress bar
			eventStream.send(new UpdateProgress(depositDetails.getJobId(), depositDetails.getDepositId(), 0,
					expectedBytes, "Starting transfer ...").withUserId(depositDetails.getUserId()));

			logger.info("Size: " + expectedBytes + " bytes (" + FileUtils.byteCountToDisplaySize(expectedBytes) + ")");

			// Progress tracking (threaded)
			Progress progress = new Progress();
			ProgressTracker tracker = new ProgressTracker(progress, depositDetails.getJobId(),
					depositDetails.getDepositId(), expectedBytes, eventStream);
			Thread trackerThread = new Thread(tracker);
			trackerThread.start();

			try {
				// Ask the driver to copy files to our working directory
				userStorage.getUserFs().retrieve(depositDetails.getFilePath(), outputFile, progress);
			} finally {
				// Stop the tracking thread
				tracker.stop();
				trackerThread.join();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private String copyTarToArchiveStorage(ArchiveStore archiveFs, DepositDetails depositDetails, File tarFile) {

		// Copy the resulting tar file to the archive area
		logger.info("Copying tar file to archive ...");

		// Progress tracking (threaded)
		Progress progress = new Progress();
		ProgressTracker tracker = new ProgressTracker(progress, depositDetails.getJobId(),
				depositDetails.getDepositId(), tarFile.length(), eventStream);
		Thread trackerThread = new Thread(tracker);
		trackerThread.start();

		String archiveId = null;

		try {
			try {
				archiveId = ((Device) archiveFs).store("/", tarFile, progress);
			} finally {
				// Stop the tracking thread
				tracker.stop();
				trackerThread.join();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		logger.info("Copied: " + progress.dirCount + " directories, " + progress.fileCount + " files, "
				+ progress.byteCount + " bytes");

		return archiveId;
	}

	private void copyBackFromArchive(ArchiveStore archiveFs, String archiveId, File tarFile) {

		// Ask the driver to copy files to the temp directory
		Progress progress = new Progress();

		try {
			((Device) archiveFs).retrieve(archiveId, tarFile, progress);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		logger.info("Copied: " + progress.dirCount + " directories, " + progress.fileCount + " files, "
				+ progress.byteCount + " bytes");
	}

	private void verifyTarFile(Path tempPath, File tarFile, String origTarHash) {

		logger.debug("Verifying Tar File - tempPath: {}, tarFile: {}, origTarHash: {}", tempPath.toString(),
				tarFile.getAbsolutePath(), origTarHash);

		if (origTarHash != null) {
			// Compare the SHA hash
			String tarHash = Verify.getDigest(tarFile);
			logger.info("Checksum: " + tarHash);
			if (!tarHash.equals(origTarHash)) {
				throw new RuntimeException("checksum failed: " + tarHash + " != " + origTarHash);
			}
		}

		// Decompress to the temporary directory
		File bagDir = Tar.unTar(tarFile, tempPath);

		// Validate the bagit directory
		if (!packager.validateBag(bagDir.toPath())) {
			throw new RuntimeException("Bag is invalid");
		} else {
			logger.info("Bag is valid");
		}

		
		try {

			// Cleanup
			logger.info("Cleaning up ... {}", bagDir);
			if (DataVaultConstants.doTempDirectoryCleanUp) {
				FileUtils.deleteDirectory(bagDir);
			}
			
		} catch (IOException e) {
			logger.error(e.getMessage());
			throw new RuntimeException(e);
		}
		
		tarFile.delete();
	}
	
	private void deleteDirectory(File dir) throws DepositException {
		// Cleanup
		logger.info("Cleaning up ... deleting: {}", dir.getAbsolutePath());
		try {
			FileUtils.deleteDirectory(dir);
		} catch (IOException io) {
			throw new DepositException("Deposit failed: Failed to delete directory: " + dir.getAbsolutePath());
		}
	}
}