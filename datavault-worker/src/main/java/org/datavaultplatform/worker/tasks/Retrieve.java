package org.datavaultplatform.worker.tasks;

import java.io.File;
import java.lang.reflect.Constructor;
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
import org.datavaultplatform.common.storage.Device;
import org.datavaultplatform.common.storage.UserStore;
import org.datavaultplatform.common.storage.Verify;
import org.datavaultplatform.common.task.Context;
import org.datavaultplatform.common.task.Task;
import org.datavaultplatform.worker.operations.IPackager;
import org.datavaultplatform.worker.operations.ProgressTracker;
import org.datavaultplatform.worker.operations.Tar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class Retrieve implements ITaskAction {
    
    private static final Logger logger = LoggerFactory.getLogger(Retrieve.class);
    
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
        
        Map<String, String> properties = task.getProperties();
        String depositId = properties.get("depositId");
        String retrieveId = properties.get("retrieveId");
        String bagID = properties.get("bagId");
        String retrievePath = properties.get("retrievePath");
        String archiveId = properties.get("archiveId");
        String userID = properties.get("userId");
        String archiveDigest = properties.get("archiveDigest");
        String archiveDigestAlgorithm = properties.get("archiveDigestAlgorithm");
        
        long archiveSize = Long.parseLong(properties.get("archiveSize"));

        if (task.isRedeliver()) {
            eventStream.send(new Error(task.getJobID(), depositId, "Retrieve stopped: the message had been redelivered, please investigate")
                .withUserId(userID));
            return;
        }
        
        ArrayList<String> states = new ArrayList<>();
        states.add("Computing free space");    // 0
        states.add("Retrieving from archive"); // 1
        states.add("Validating data");         // 2
        states.add("Transferring files");      // 3
        states.add("Data retrieve complete");  // 4
        eventStream.send(new InitStates(task.getJobID(), depositId, states)
            .withUserId(userID));
        
        eventStream.send(new RetrieveStart(task.getJobID(), depositId, retrieveId)
            .withUserId(userID)
            .withNextState(0));
        
        logger.info("bagID: " + bagID);
        logger.info("retrievePath: " + retrievePath);
        
        Device userFs;
        UserStore userStore;
        Device archiveFs;
        
        // Connect to the user storage
        try {
            Class<?> clazz = Class.forName(task.getUserFileStore().getStorageClass());
            Constructor<?> constructor = clazz.getConstructor(String.class, Map.class);
            Object instance = constructor.newInstance(task.getUserFileStore().getStorageClass(), task.getUserFileStore().getProperties());
            userFs = (Device)instance;
            userStore = (UserStore)userFs;
        } catch (Exception e) {
            String msg = "Retrieve failed: could not access active filesystem";
            logger.error(msg, e);
            eventStream.send(new Error(task.getJobID(), depositId, msg)
                .withUserId(userID));
            return;
        }
        
        // Connect to the archive storage
        try {
            Class<?> clazz = Class.forName(task.getArchiveFileStore().getStorageClass());
            Constructor<?> constructor = clazz.getConstructor(String.class, Map.class);
            Object instance = constructor.newInstance(task.getArchiveFileStore().getStorageClass(), task.getArchiveFileStore().getProperties());
            archiveFs = (Device)instance;
        } catch (Exception e) {
            String msg = "Retrieve failed: could not access archive filesystem";
            logger.error(msg, e);
            eventStream.send(new Error(task.getJobID(), depositId, msg)
                .withUserId(userID));
            return;
        }
        
        try {
            if (!userStore.exists(retrievePath) || !userStore.isDirectory(retrievePath)) {
                // Target path must exist and be a directory
                logger.info("Target directory not found!");
            }
            
            // Check that there's enough free space ...
            try {
                long freespace = userFs.getUsableSpace();
                logger.info("Free space: " + freespace + " bytes (" +  FileUtils.byteCountToDisplaySize(freespace) + ")");
                if (freespace < archiveSize) {
                    eventStream.send(new Error(task.getJobID(), depositId, "Not enough free space to retrieve data!")
                        .withUserId(userID));
                    return;
                }
            } catch (Exception e) {
                logger.info("Unable to determine free space");
                eventStream.send(new Error(task.getJobID(), depositId, "Unable to determine free space")
                    .withUserId(userID));
            }

            // Retrieve the archived data
            String tarFileName = bagID + ".tar";
            
            // Copy the tar file from the archive to the temporary area
            Path tarPath = context.getTempDir().resolve(tarFileName);
            File tarFile = tarPath.toFile();
            
            eventStream.send(new UpdateProgress(task.getJobID(), depositId, 0, archiveSize, "Starting transfer ...")
                .withUserId(userID)
                .withNextState(1));
            
            // Progress tracking (threaded)
            Progress progress = new Progress();
            ProgressTracker tracker = new ProgressTracker(progress, task.getJobID(), depositId, archiveSize, eventStream);
            Thread trackerThread = new Thread(tracker);
            trackerThread.start();

            try {
                // Ask the driver to copy files to the temp directory
                archiveFs.retrieve(archiveId, tarFile, progress);
            } finally {
                // Stop the tracking thread
                tracker.stop();
                trackerThread.join();
            }
            
            logger.info("Copied: " + progress.dirCount + " directories, " + progress.fileCount + " files, " + progress.byteCount + " bytes");
            
            logger.info("Validating data ...");
            eventStream.send(new UpdateProgress(task.getJobID(), depositId).withNextState(2)
                .withUserId(userID));
            
            // Verify integrity with deposit checksum
            String systemAlgorithm = Verify.getAlgorithm();
            if (!systemAlgorithm.equals(archiveDigestAlgorithm)) {
                throw new Exception("Unsupported checksum algorithm: " + archiveDigestAlgorithm);
            }
            
            String tarHash = Verify.getDigest(tarFile);
            logger.info("Checksum algorithm: " + archiveDigestAlgorithm);
            logger.info("Checksum: " + tarHash);
            
            if (!tarHash.equals(archiveDigest)) {
                throw new Exception("checksum failed: " + tarHash + " != " + archiveDigest);
            }
            
            // Decompress to the temporary directory
            File bagDir = Tar.unTar(tarFile, context.getTempDir());
            long bagDirSize = FileUtils.sizeOfDirectory(bagDir);
            
            // Validate the bagit directory
            if (!packager.validateBag(bagDir)) {
                throw new Exception("Bag is invalid");
            }
            
            // Get the payload data directory
            // File payloadDir = bagDir.toPath().resolve("data").toFile();
            // long payloadSize = FileUtils.sizeOfDirectory(payloadDir);
            
            // Copy the extracted files to the target retrieve area
            logger.info("Copying to user directory ...");
            eventStream.send(new UpdateProgress(task.getJobID(), depositId, 0, bagDirSize, "Starting transfer ...")
                .withUserId(userID)
                .withNextState(3));
            
            // Progress tracking (threaded)
            progress = new Progress();
            tracker = new ProgressTracker(progress, task.getJobID(), depositId, bagDirSize, eventStream);
            trackerThread = new Thread(tracker);
            trackerThread.start();
            
            try {
                // Ask the driver to copy files to the user directory
                userFs.store(retrievePath, bagDir, progress);
            } finally {
                // Stop the tracking thread
                tracker.stop();
                trackerThread.join();
            }
            
            logger.info("Copied: " + progress.dirCount + " directories, " + progress.fileCount + " files, " + progress.byteCount + " bytes");
            
            // Cleanup
            logger.info("Cleaning up ...");
            FileUtils.deleteDirectory(bagDir);
            tarFile.delete();
            
            logger.info("Data retrieve complete: " + retrievePath);
            eventStream.send(new RetrieveComplete(task.getJobID(), depositId, retrieveId).withNextState(4)
                .withUserId(userID));
            
        } catch (Exception e) {
            String msg = "Data retrieve failed: " + e.getMessage();
            logger.error(msg, e);
            eventStream.send(new Error(task.getJobID(), depositId, msg)
                .withUserId(userID));
        }
    }
}
