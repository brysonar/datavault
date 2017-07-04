package org.datavaultplatform.worker.tasks;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.datavaultplatform.common.event.Error;
import org.datavaultplatform.common.event.EventStream;
import org.datavaultplatform.common.event.InitStates;
import org.datavaultplatform.common.event.UpdateProgress;
import org.datavaultplatform.common.event.retrieve.RetrieveComplete;
import org.datavaultplatform.common.event.retrieve.RetrieveStart;
import org.datavaultplatform.common.io.Progress;
import org.datavaultplatform.common.storage.CheckSumEnum;
import org.datavaultplatform.common.storage.Device;
import org.datavaultplatform.common.storage.UserStore;
import org.datavaultplatform.common.task.Context;
import org.datavaultplatform.common.task.Task;
import org.datavaultplatform.worker.exception.RetrieveException;
import org.datavaultplatform.worker.model.RetrieveDetails;
import org.datavaultplatform.worker.model.UserStorage;
import org.datavaultplatform.worker.operations.IPackager;
import org.datavaultplatform.worker.operations.ProgressTracker;
import org.datavaultplatform.worker.operations.Tar;
import org.datavaultplatform.worker.util.CheckSumUtil;
import org.datavaultplatform.worker.util.DataVaultConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class Retrieve implements ITaskAction {
    
    private static final Logger logger = LoggerFactory.getLogger(Retrieve.class);
    
	public final CheckSumEnum tarCheckSumEnum = DataVaultConstants.TAR_CHECKSUM_ENUM;
	
	private final IPackager packager;
	private final EventStream eventStream;
	
	@Autowired
	public Retrieve(IPackager packager, EventStream eventStream) {
		super();
		this.packager = packager;
		this.eventStream = eventStream;
	}

	
	@Override
	public void performAction(Context context, Task task) {

		logger.info("Retrieve job - performAction()");

		RetrieveDetails retrieveDetails = new RetrieveDetails().build(task);

		try {
			
			doAction(context, task, retrieveDetails);

		} catch (RetrieveException e) {
			String msg = e.getMessage();
			logger.error(msg, e);
			eventStream.send(new Error(task.getJobID(), retrieveDetails.getDepositId(), msg)
					.withUserId(retrieveDetails.getUserID()));
			return;
		}
	}
    
	
    private void doAction(Context context, Task task, RetrieveDetails retrieveDetais) throws RetrieveException {

        stopProcessingIfRedeliver(task);
        
        ArrayList<String> states = buildStates();
        
        eventStream.send(new InitStates(task.getJobID(), retrieveDetais.getDepositId(), states)
            .withUserId(retrieveDetais.getUserID()));
        
        eventStream.send(new RetrieveStart(task.getJobID(), retrieveDetais.getDepositId(), retrieveDetais.getRetrieveId())
            .withUserId(retrieveDetais.getUserID())
            .withNextState(0));
        
        logger.info("BagId: {}, retrievePath", retrieveDetais.getBagId(), retrieveDetais.getRetrievePath());

        UserStorage userStorage = connectToUserStorage(task);
        Device archiveFs = connectToArchiveStorage(task);

        try {
            validateTargetDirectory(retrieveDetais, userStorage);
            validateEnoughFreeSpace(task, retrieveDetais, userStorage);

            // Retrieve the archived data
            String tarFileName = retrieveDetais.getBagId() + ".tar";
            
            // Copy the tar file from the archive to the temporary area
            Path tarTempPath = context.getTempDir().resolve(tarFileName);
            File tarTempFile = tarTempPath.toFile();

			copyFilesToTempDirectory(task, retrieveDetais, archiveFs, tarTempFile);
            validateTarChecksum(task, retrieveDetais, tarTempFile);
            
            // Decompress to the temporary directory
            File bagDir = Tar.unTar(tarTempFile, context.getTempDir());

            // Validate the bagit directory
            if (!packager.validateBag(bagDir.toPath())) {
                throw new RuntimeException("Bag is invalid");
            }

            copyExtractedFilesToTargetRetrieveArea(task, retrieveDetais, userStorage, bagDir);
            
            // Cleanup
            logger.info("Cleaning up ...");
            if (DataVaultConstants.doTempDirectoryCleanUp) {
            	FileUtils.deleteDirectory(bagDir);
            	tarTempFile.delete();
            }
            
            logger.info("Data retrieve complete: " + retrieveDetais.getRetrievePath());
            eventStream.send(new RetrieveComplete(task.getJobID(), retrieveDetais.getDepositId(), retrieveDetais.getRetrieveId()).withNextState(4)
                .withUserId(retrieveDetais.getUserID()));
            
        } catch (Exception e) {
            String errMsg = "Data retrieve failed -  " + e.getMessage();
            throw new RetrieveException(errMsg, e);
        }
    }


	private void copyExtractedFilesToTargetRetrieveArea(Task task, RetrieveDetails retrieveDetais,
			UserStorage userStorage, File bagDir) throws Exception, InterruptedException {
		// Copy the extracted files to the target retrieve area
		
		logger.info("Copying to user directory ...");
		
		long bagDirSize = FileUtils.sizeOfDirectory(bagDir);
		
		eventStream.send(new UpdateProgress(task.getJobID(), retrieveDetais.getDepositId(), 0, bagDirSize, "Starting transfer ...")
		    .withUserId(retrieveDetais.getUserID())
		    .withNextState(3));
		
		// Progress tracking (threaded)
		Progress progress = new Progress();
		ProgressTracker tracker = new ProgressTracker(progress, task.getJobID(), retrieveDetais.getDepositId(), bagDirSize, eventStream);
		Thread trackerThread = new Thread(tracker);
		trackerThread.start();
		
		try {
		    // Ask the driver to copy files to the user directory
			userStorage.getUserFs().store(retrieveDetais.getRetrievePath(), bagDir, progress);
		} finally {
		    // Stop the tracking thread
		    tracker.stop();
		    trackerThread.join();
		}
		
		logger.info("Copied: " + progress.dirCount + " directories, " + progress.fileCount + " files, " + progress.byteCount + " bytes");
	}


	private void validateTarChecksum(Task task, RetrieveDetails retrieveDetais, File tarTempFile) throws Exception {
		logger.info("Validating data ...");
		eventStream.send(new UpdateProgress(task.getJobID(), retrieveDetais.getDepositId()).withNextState(2)
		    .withUserId(retrieveDetais.getUserID()));
		
		// Verify integrity with deposit checksum
		String systemAlgorithm = tarCheckSumEnum.getJavaSecurityAlgorithm();
		if (!systemAlgorithm.equals(retrieveDetais.getArchiveDigestAlgorithm())) {
		    throw new Exception("Unsupported checksum algorithm: " + retrieveDetais.getArchiveDigestAlgorithm());
		}
		
		String tarTempHash = CheckSumUtil.getDigest(tarTempFile, tarCheckSumEnum);
		logger.info("Checksum type: " + retrieveDetais.getArchiveDigestAlgorithm());
		logger.info("Checksum of tar copied to temp directory: " + tarTempHash);
		
		if (!tarTempHash.equals(retrieveDetais.getArchiveDigest())) {
		    throw new RuntimeException("Checksum validation failed - tarfile checksum: " + tarTempHash + " != expected: " + retrieveDetais.getArchiveDigest());
		}
	}


	private void copyFilesToTempDirectory(Task task, RetrieveDetails retrieveDetais, Device archiveFs, File tarTempFile)
			throws Exception, InterruptedException {
		
        eventStream.send(new UpdateProgress(task.getJobID(), retrieveDetais.getDepositId(), 0, retrieveDetais.getArchiveSize(), "Starting transfer ...")
                .withUserId(retrieveDetais.getUserID())
                .withNextState(1));
        
		// Progress tracking (threaded)
		Progress progress = new Progress();
		ProgressTracker tracker = new ProgressTracker(progress, task.getJobID(), retrieveDetais.getDepositId(), retrieveDetais.getArchiveSize(), eventStream);
		Thread trackerThread = new Thread(tracker);
		trackerThread.start();

		try {
		    // Ask the driver to copy files to the temp directory
		    archiveFs.retrieve(retrieveDetais.getArchiveId(), tarTempFile, progress);
		} finally {
		    // Stop the tracking thread
		    tracker.stop();
		    trackerThread.join();
		}
		
		logger.info("Copied: " + progress.dirCount + " directories, " + progress.fileCount + " files, " + progress.byteCount + " bytes");
	}


	private void validateEnoughFreeSpace(Task task, RetrieveDetails retrieveDetais, UserStorage userStorage)
			throws RetrieveException {
		// Check that there's enough free space ...
		long freespace = 0;
		boolean isFreeSpaceUndetermined = false;
		try {
		    freespace = userStorage.getUserFs().getUsableSpace();
		    logger.info("Free space: " + freespace + " bytes (" +  FileUtils.byteCountToDisplaySize(freespace) + ")");

		} catch (RuntimeException e) {
		    logger.info("Unable to determine free space");
		    isFreeSpaceUndetermined = true;
		    eventStream.send(new Error(task.getJobID(), retrieveDetais.getDepositId(), "Unable to determine free space")
		        .withUserId(retrieveDetais.getUserID()));
		}
		
		if (!isFreeSpaceUndetermined) {
			if (freespace < retrieveDetais.getArchiveSize()) {
				String errMsg = "Not enough free space to retrieve data!";
				throw new RetrieveException(errMsg);
			}
		}
	}


	private void validateTargetDirectory(RetrieveDetails retrieveDetais, UserStorage userStorage)
			throws RetrieveException {

		if (!userStorage.getUserStore().exists(retrieveDetais.getRetrievePath())
				|| !userStorage.getUserStore().isDirectory(retrieveDetais.getRetrievePath())) {
			// Target path must exist and be a directory
			String errMsg = "Target directory not found!";
			throw new RetrieveException(errMsg);
		}

	}


	private UserStorage connectToUserStorage(Task task) throws RetrieveException {
		
		// Connect to the user storage
        try {
            Class<?> clazz = Class.forName(task.getUserFileStore().getStorageClass());
            Constructor<?> constructor = clazz.getConstructor(String.class, Map.class);
            Object instance = constructor.newInstance(task.getUserFileStore().getStorageClass(), task.getUserFileStore().getProperties());
            Device userFs = (Device) instance;
            UserStore userStore = (UserStore) userFs;
            return new UserStorage(userFs, userStore);
            
        } catch (Exception e) {
            String errMsg = "Retrieve failed: could not access active filesystem";
			throw new RetrieveException(errMsg);
        }
	}

	private Device connectToArchiveStorage(Task task) throws RetrieveException {
		
		// Connect to the archive storage
		Device archiveFs = null;
        try {
            Class<?> clazz = Class.forName(task.getArchiveFileStore().getStorageClass());
            Constructor<?> constructor = clazz.getConstructor(String.class, Map.class);
            Object instance = constructor.newInstance(task.getArchiveFileStore().getStorageClass(), task.getArchiveFileStore().getProperties());
            archiveFs = (Device)instance;
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            String errMsg = "Retrieve failed: could not access archive filesystem";
            throw new RetrieveException(errMsg, e);
		}
		return archiveFs;
	}
	
	private void stopProcessingIfRedeliver(Task task) throws RetrieveException {
		if (task.isRedeliver()) {
            String errMsg = "Retrieve stopped: the message had been redelivered, please investigate";
			throw new RetrieveException(errMsg);
        }
	}

	private ArrayList<String> buildStates() {
		ArrayList<String> states = new ArrayList<>();
        states.add("Computing free space");    // 0
        states.add("Retrieving from archive"); // 1
        states.add("Validating data");         // 2
        states.add("Transferring files");      // 3
        states.add("Data retrieve complete");  // 4
		return states;
	}
}
