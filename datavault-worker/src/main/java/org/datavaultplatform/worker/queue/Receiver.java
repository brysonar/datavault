package org.datavaultplatform.worker.queue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.datavaultplatform.common.task.Context;
import org.datavaultplatform.common.task.Task;
import org.datavaultplatform.worker.WorkerInstance;
import org.datavaultplatform.worker.exception.DataVaultWorkerException;
import org.datavaultplatform.worker.tasks.ITaskAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

public class Receiver {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private TaskBuilder taskBuilder;

	@Autowired
	private TaskFactory taskFactory;
	
	@Value("${tempDir}")
    private String tempDir;
	
	@Value("${metaDir}")
	private String metaDir;
    
	public void process(String message, boolean isRedeliver) {
		
		logger.debug("Received message: {}", message);
		 
		try {
			Task task = taskBuilder.build(message, isRedeliver);
			ITaskAction taskAction = taskFactory.getTaskAction(task);
			
			Path tempDirPath = getTempDirPath();
			createTempDirectory(tempDirPath);

			Path metaDirPath = Paths.get(metaDir);
			logger.debug("Meta Directory: {}", metaDirPath.toFile().getAbsolutePath());

			Context context = new Context(tempDirPath, metaDirPath, null);

			taskAction.performAction(context, task);
			
			//TODO this will not be deleted if an earlier exception has been thrown
			//deleteTempDirectory(tempDirPath);

		} catch (RuntimeException e) {
			logger.error("Error decoding message: " + e.getMessage(), e);
		}
	}

	private Path getTempDirPath() {
		Path tempDirPath = Paths.get(tempDir, WorkerInstance.getWorkerName());
		return tempDirPath;
	}

	private void createTempDirectory(Path tempDirPath) {
		logger.debug("Creating Temp Directory: {}", tempDirPath.toString());
		File tempDirectory = tempDirPath.toFile();
		tempDirectory.mkdir();
		// TODO throw exception if directory not created
	}

	private void deleteTempDirectory(Path tempDirPath) {
		// Clean up the temporary directory
		try {
			FileUtils.deleteDirectory(tempDirPath.toFile());
		} catch (IOException e) {
			throw new DataVaultWorkerException("Failed to delete tempDirectory: " + tempDirPath, e);
		}
	}

}
