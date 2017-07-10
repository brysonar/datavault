package org.datavaultplatform.worker.tasks;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.datavaultplatform.common.event.Error;
import org.datavaultplatform.common.event.EventStream;
import org.datavaultplatform.common.event.InitStates;
import org.datavaultplatform.common.event.UpdateProgress;
import org.datavaultplatform.common.event.retrieve.RetrieveComplete;
import org.datavaultplatform.common.event.retrieve.RetrieveStart;
import org.datavaultplatform.common.io.Progress;
import org.datavaultplatform.common.model.FileStore;
import org.datavaultplatform.common.storage.ArchiveStore;
import org.datavaultplatform.common.storage.CheckSumEnum;
import org.datavaultplatform.common.storage.StorageFactory;
import org.datavaultplatform.common.storage.UserStore;
import org.datavaultplatform.common.task.Context;
import org.datavaultplatform.common.task.Task;
import org.datavaultplatform.worker.exception.RetrieveException;
import org.datavaultplatform.worker.model.RetrieveDetails;
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
    private final StorageFactory storageFactory;
	
	@Autowired
	public Retrieve(IPackager packager,  StorageFactory storageFactory, EventStream eventStream) {
		super();
		this.packager = packager;
		this.eventStream = eventStream;
		this.storageFactory = storageFactory;
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
    
	
    private void doAction(Context context, Task task, RetrieveDetails retrieveDetails) throws RetrieveException {

        stopProcessingIfRedeliver(task);
        
        ArrayList<String> states = buildStates();
        
        eventStream.send(new InitStates(task.getJobID(), retrieveDetails.getDepositId(), states)
            .withUserId(retrieveDetails.getUserID()));
        
        eventStream.send(new RetrieveStart(task.getJobID(), retrieveDetails.getDepositId(), retrieveDetails.getRetrieveId())
            .withUserId(retrieveDetails.getUserID())
            .withNextState(0));
        
        logger.info("BagId: {}, retrievePath", retrieveDetails.getBagId(), retrieveDetails.getRetrievePath());

        try {
        	
            UserStore userStore = getUserStorage(task.getUserFileStore());
            ArchiveStore archiveStore = getArchiveStorage(task.getArchiveFileStore());
            
            validateTargetDirectory(retrieveDetails, userStore);
            validateEnoughFreeSpace(task, retrieveDetails, userStore);

            // Retrieve the archived data
            String tarFileName = retrieveDetails.getBagId() + ".tar";
            
            // Copy the tar file from the archive to the temporary area
            Path tarTempPath = context.getTempDir().resolve(tarFileName);
            File tarTempFile = tarTempPath.toFile();

            copyArchivedFilesToTempDirectory(task, retrieveDetails, archiveStore, tarTempFile);
            validateTarChecksum(task, retrieveDetails, tarTempFile);
            
            // Decompress to the temporary directory
            File bagDir = Tar.unTar(tarTempFile, context.getTempDir());

            // Validate the bagit directory
            if (!packager.validateBag(bagDir.toPath())) {
                throw new RuntimeException("Bag is invalid");
            }

            copyExtractedFilesToUserArea(task, retrieveDetails, userStore, bagDir);
            
            // Cleanup
            logger.info("Cleaning up ...");
            if (DataVaultConstants.doTempDirectoryCleanUp) {
            	FileUtils.deleteDirectory(bagDir);
            	tarTempFile.delete();
            }
            
            logger.info("Data retrieve complete: " + retrieveDetails.getRetrievePath());
            eventStream.send(new RetrieveComplete(task.getJobID(), retrieveDetails.getDepositId(), retrieveDetails.getRetrieveId()).withNextState(4)
                .withUserId(retrieveDetails.getUserID()));
            
        } catch (Exception e) {
            String errMsg = "Retrieve failed -  " + e.getMessage();
            throw new RetrieveException(errMsg, e);
        }
    }


	private void copyExtractedFilesToUserArea(Task task, RetrieveDetails retrieveDetais,
			UserStore userStore, File bagDir) throws Exception, InterruptedException {
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
			userStore.store(retrieveDetais.getRetrievePath(), bagDir, progress);
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


	private void copyArchivedFilesToTempDirectory(Task task, RetrieveDetails retrieveDetais, ArchiveStore archiveStore, File tarTempFile)
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
			archiveStore.retrieve(retrieveDetais.getArchiveId(), tarTempFile, progress);
		} finally {
		    // Stop the tracking thread
		    tracker.stop();
		    trackerThread.join();
		}
		
		logger.info("Copied: " + progress.dirCount + " directories, " + progress.fileCount + " files, " + progress.byteCount + " bytes");
	}


	private void validateEnoughFreeSpace(Task task, RetrieveDetails retrieveDetais, UserStore userStore)
			throws RetrieveException {
		// Check that there's enough free space ...
		long freespace = 0;
		boolean isFreeSpaceUndetermined = false;
		try {
		    freespace = userStore.getUsableSpace();
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


	private void validateTargetDirectory(RetrieveDetails retrieveDetais, UserStore userStore)
			throws RetrieveException {

		if (!userStore.exists(retrieveDetais.getRetrievePath())
				|| !userStore.isDirectory(retrieveDetais.getRetrievePath())) {
			// Target path must exist and be a directory
			String errMsg = "Target directory not found!";
			throw new RetrieveException(errMsg);
		}

	}


	private UserStore getUserStorage(FileStore userFileStore) {
		
		// Connect to the user storage
		//return getUserStorage(userFileStore);
		return storageFactory.getUserStore(userFileStore.getStorageClass(), userFileStore.getStorageClass(), userFileStore.getProperties());
	}

	private ArchiveStore getArchiveStorage(org.datavaultplatform.common.model.ArchiveStore archiveStoreModel) {
		
//		// Connect to the archive storage
//		return storageFactory.getArchiveStorage(archiveStoreModel);
		return storageFactory.getArchiveStorage(archiveStoreModel.getStorageClass(), archiveStoreModel.getStorageClass(), archiveStoreModel.getProperties());
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
