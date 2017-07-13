package org.datavaultplatform.worker.queue;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.datavaultplatform.common.task.Context;
import org.datavaultplatform.common.task.Task;
import org.datavaultplatform.common.util.DataVaultConstants;
import org.datavaultplatform.worker.WorkerInstance;
import org.datavaultplatform.worker.tasks.ITaskAction;
import org.datavaultplatform.worker.util.FileUtil;
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
		 
		Path tempDirPath = null;
		
		try {
			Task task = taskBuilder.build(message, isRedeliver);
			ITaskAction taskAction = taskFactory.getTaskAction(task);
			
			tempDirPath = getTempDirPath();
			FileUtil.createDirectory(tempDirPath);

			Path metaDirPath = Paths.get(metaDir);
			logger.debug("Meta Directory: {}", metaDirPath.toFile().getAbsolutePath());

			Context context = new Context(tempDirPath, metaDirPath, null);
			taskAction.performAction(context, task);

		} catch (RuntimeException e) {
			logger.error("Error decoding message: " + e.getMessage(), e);
		} finally {
			try {
			deleteTempDirectory(tempDirPath);
			} catch (RuntimeException e){
				logger.error(e.getMessage(), e);
			}
		}
	}

	private void deleteTempDirectory(Path tempDirPath) {
		if (DataVaultConstants.doTempDirectoryCleanUp) {
			if (tempDirPath != null) {
				FileUtil.deleteDirectory(tempDirPath);
			}
		}
	}

	private Path getTempDirPath() {
		Path tempDirPath = Paths.get(tempDir, WorkerInstance.getWorkerName());
		return tempDirPath;
	}

}
