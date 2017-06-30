package org.datavaultplatform.worker.queue;

import java.io.IOException;

import org.datavaultplatform.common.task.Task;
import org.datavaultplatform.worker.exception.TaskBuilderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TaskFactoryBuilder implements TaskBuilder {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());


	@Override
	public Task build(String message, boolean isRedeliver) {

		Task task = buildTask(message);
		
		// Is the message a redelivery?
		if (isRedeliver) {
			task.setIsRedeliver(isRedeliver);
		}
		
		return task;
		
	}

	private Task buildTask(String message) {
		Task task;
		try {
			ObjectMapper mapper = new ObjectMapper();
			task = mapper.readValue(message, Task.class);
			logger.debug("Task: {}", task);

		} catch (IOException | RuntimeException e) {
			throw new TaskBuilderException("Failed to map message to task", e);
		}
		return task;
	}

}
