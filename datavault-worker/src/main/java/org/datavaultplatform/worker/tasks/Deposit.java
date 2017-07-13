package org.datavaultplatform.worker.tasks;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;

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
import org.datavaultplatform.common.storage.StorageFactory;
import org.datavaultplatform.common.storage.UserStore;
import org.datavaultplatform.common.storage.VerifyMethod;
import org.datavaultplatform.common.task.Context;
import org.datavaultplatform.common.task.Task;
import org.datavaultplatform.common.util.DataVaultConstants;
import org.datavaultplatform.common.util.TarUtil;
import org.datavaultplatform.worker.exception.DataVaultWorkerException;
import org.datavaultplatform.worker.exception.DepositException;
import org.datavaultplatform.worker.model.DepositDetails;
import org.datavaultplatform.worker.model.MetaData;
import org.datavaultplatform.worker.model.Storage;
import org.datavaultplatform.worker.operations.IPackager;
import org.datavaultplatform.worker.operations.Identifier;
import org.datavaultplatform.worker.operations.ProgressTracker;
import org.datavaultplatform.worker.util.CheckSumUtil;
import org.datavaultplatform.worker.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Deposit implements ITaskAction {

	private static final String DEPOSIT_FAILED_PREFIX = "Deposit failed - ";

	private final static Logger logger = LoggerFactory.getLogger(Deposit.class);

	private final IPackager packager;
	private final EventStream eventStream;
    private final StorageFactory storageFactory;

	//public final CheckSumEnum checkSumEnum = DataVaultConstants.CHECKSUM_ENUM;
	public final CheckSumEnum tarCheckSumEnum = DataVaultConstants.TAR_CHECKSUM_ENUM;
	
	@Autowired
	public Deposit(IPackager packager, StorageFactory storageFactory, EventStream eventStream) {
		super();
		this.packager = packager;
		this.eventStream = eventStream;
		this.storageFactory = storageFactory;
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
				errMsg = DEPOSIT_FAILED_PREFIX + e.getMessage();
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

		logger.info("Bag Id: {}, Deposit file: {}", depositDetails.getBagId(), depositDetails.getFilePath());

		Storage storage = buildStorageConnections(task);

		checkUserStoreExists(storage.getUserStore(), depositDetails);

		Path tempBagPath = createTempBagDirectory(context.getTempDir(), depositDetails);

		// Copy the target file to the bag directory
		eventStream.send(new UpdateProgress(depositDetails.getJobId(), depositDetails.getDepositId())
				.withUserId(depositDetails.getUserId()).withNextState(1));

		Path tempBagDataPath = createTempBagDataDirectory(tempBagPath);

		copyFromUserStorageToTempDataDirectory(storage.getUserStore(), depositDetails, tempBagDataPath);

		// Bag the directory in-place
		eventStream.send(new TransferComplete(depositDetails.getJobId(), depositDetails.getDepositId())
				.withUserId(depositDetails.getUserId()).withNextState(2));

		packager.createBag(tempBagPath, tempBagDataPath);

		buildMetaData(metaData, tempBagPath, tempBagDataPath);

		File tarFile = generateTar(context.getTempDir(), depositDetails.getBagId(), tempBagPath);
		
		String tarHash = CheckSumUtil.getDigest(tarFile, tarCheckSumEnum);
		logger.info("Tar file Checksum: " + tarHash + " using " + tarCheckSumEnum.getJavaSecurityAlgorithm());

		long archiveSize = tarFile.length();
		logger.info("Tar file: " + archiveSize + " bytes");

		//TODO review with Robin to see if we need this. It is not added to the file in the tar only metadata directory
		//the checksum has is sent with receiver so must be stored in dbase
		packager.addTarfileChecksum(tempBagPath, tarFile.toPath(), tarHash, tarCheckSumEnum);
		
		eventStream.send(new PackageComplete(depositDetails.getJobId(), depositDetails.getDepositId())
				.withUserId(depositDetails.getUserId()).withNextState(3));

		eventStream.send(
				new ComputedDigest(depositDetails.getJobId(), depositDetails.getDepositId(), tarHash, tarCheckSumEnum.getJavaSecurityAlgorithm())
						.withUserId(depositDetails.getUserId()));

		copyToMetaDirectory(context, depositDetails, tempBagPath.toFile());

		String archiveId = copyTarToArchiveStorage(storage.getArchiveStore(), depositDetails, tarFile);

		// TODO there is multiple deletes of this directory - see verify archive
		//if (DataVaultConstants.doTempDirectoryCleanUp) {
		FileUtil.deleteDirectory(tempBagDataPath.toFile());
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

		TarUtil.createTar(tempBagPath.toFile(), tarFile);
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
		logger.info("Verifying archive package: {}", tarFile.getAbsolutePath());

		ArchiveStore archiveStore = storage.getArchiveStore();

		logger.info("Verification method: " + archiveStore.getVerifyMethod());
		if (archiveStore.getVerifyMethod() == VerifyMethod.LOCAL_ONLY) {

			// Verify the contents of the temporary file
			verifyTarFile(context.getTempDir(), tarFile, null);

		} else if (archiveStore.getVerifyMethod() == VerifyMethod.COPY_BACK) {

			// Delete the existing temporary file
			if (tarFile.exists()) {
				logger.debug("About to delete tar file: {}", tarFile.getAbsolutePath());
				tarFile.delete();
			}

			if (archiveStore.isEncryptionEnabled()) {
				//delete encrypted file from temp 
				File encryptedFile = tarFile.getParentFile().toPath().resolve(tarFile.getName() + DataVaultConstants.ENCRYPT_SUFFIX).toFile();
				if (encryptedFile.exists()) {
					encryptedFile.delete();
				}
			}
			
			// Copy file back from the archive storage
			copyBackFromArchive(storage.getArchiveStore(), archiveId, tarFile);

			// Verify the contents
			verifyTarFile(context.getTempDir(), tarFile, tarHash);
		}
	}



	private void checkUserStoreExists(UserStore userStore, DepositDetails depositDetails) {

		if (!userStore.exists(depositDetails.getFilePath())) {
			String errMsg = "User Store not found";
			throw new DataVaultWorkerException(errMsg);
		}
	}

	private Storage buildStorageConnections(Task task) {
		UserStore userStore = getUserStorage(task.getUserFileStore());
		ArchiveStore archiveStore = getArchiveStorage(task.getArchiveFileStore());
		Storage storage = new Storage(userStore, archiveStore);
		return storage;
	}
	
	private UserStore getUserStorage(FileStore userFileStore) {
		// Connect to the user storage
//        return getUserStorage(userFileStore);
		return storageFactory.getUserStore(userFileStore.getStorageClass(), userFileStore.getStorageClass(), userFileStore.getProperties());
	}
	
	private ArchiveStore getArchiveStorage(org.datavaultplatform.common.model.ArchiveStore archiveFileStore) {
		// Connect to the archive storage
		// return storageFactory.getArchiveStorage(archiveFileStore);
		return storageFactory.getArchiveStorage(archiveFileStore.getStorageClass(), archiveFileStore.getStorageClass(), archiveFileStore.getProperties());
	}


	private void processRedeliver(boolean isRedeliver) throws DepositException {
		if (isRedeliver) {
			String errMsg = "Deposit stopped - The message had been redelivered, please investigate";
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

	private void copyFromUserStorageToTempDataDirectory(UserStore userStore, DepositDetails depositDetails,
			Path bagDataPath) {

		logger.info("Copying target from: {}, to bag directory: {}", depositDetails.getFilePath(), bagDataPath);
		try {
			String fileName = userStore.getName(depositDetails.getFilePath());
			File outputFile = bagDataPath.resolve(fileName).toFile();

			// Compute bytes to copy
			long expectedBytes = userStore.getSize(depositDetails.getFilePath());
			
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
				userStore.retrieveFromUserStore(depositDetails.getFilePath(), outputFile, progress);
			} finally {
				// Stop the tracking thread
				tracker.stop();
				trackerThread.join();
			}
		} catch (Exception e) {
			throw new DataVaultWorkerException(e.getMessage(), e);
		}
	}

	private String copyTarToArchiveStorage(ArchiveStore archiveStore, DepositDetails depositDetails, File tarFile) {

		// Copy the resulting tar file to the archive area
		logger.info("Copying tar file to archive: {}", tarFile.getAbsolutePath());

		// Progress tracking (threaded)
		Progress progress = new Progress();
		ProgressTracker tracker = new ProgressTracker(progress, depositDetails.getJobId(),
				depositDetails.getDepositId(), tarFile.length(), eventStream);
		Thread trackerThread = new Thread(tracker);
		trackerThread.start();

		String archiveId = null;

		try {
			try {
				archiveId = archiveStore.storeToArchive("/", tarFile, progress);
			} finally {
				// Stop the tracking thread
				tracker.stop();
				trackerThread.join();
			}
		} catch (Exception e) {
			throw new DataVaultWorkerException(e.getMessage(), e);
		}

		logger.info("Copied: " + progress.dirCount + " directories, " + progress.fileCount + " files, "
				+ progress.byteCount + " bytes");

		return archiveId;
	}

	private void copyBackFromArchive(ArchiveStore archiveStore, String archiveId, File file) {

		// Ask the driver to copy files to the temp directory
		Progress progress = new Progress();

		try {
			archiveStore.retrieveFromArchive(archiveId, file, progress);
		} catch (Exception e) {
			throw new DataVaultWorkerException(e.getMessage(), e);
		}
		logger.info("Copied: " + progress.dirCount + " directories, " + progress.fileCount + " files, "
				+ progress.byteCount + " bytes");
	}

	private void verifyTarFile(Path tempPath, File tarFile, String origTarHash) {

		logger.debug("Verifying Tar File - tempPath: {}, tarFile: {}, origTarHash: {}", tempPath.toString(),
				tarFile.getAbsolutePath(), origTarHash);

		if (origTarHash != null) {
			// Compare the SHA hash
			String tarHash = CheckSumUtil.getDigest(tarFile, tarCheckSumEnum);
			logger.info("Checksum: " + tarHash);
			if (!tarHash.equals(origTarHash)) {
				throw new DataVaultWorkerException("Tar file checksum verificiation failed: " + tarHash + " != " + origTarHash);
			}
		}

		// Decompress to the temporary directory
		File bagDir = TarUtil.unTar(tarFile, tempPath);

		// Validate the bagit directory
		if (!packager.validateBag(bagDir.toPath())) {
			throw new DataVaultWorkerException("Bag is invalid");
		}
			
		logger.info("Bag is valid");

		if (DataVaultConstants.doTempDirectoryCleanUp) {
			FileUtil.deleteDirectory(bagDir);
		}
		
		tarFile.delete();
	}
	

}