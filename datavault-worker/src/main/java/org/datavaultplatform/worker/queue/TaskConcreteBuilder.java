package org.datavaultplatform.worker.queue;

import java.io.IOException;

import org.datavaultplatform.common.task.Task;
import org.datavaultplatform.worker.exception.TaskBuilderException;
import org.datavaultplatform.worker.operations.IPackager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TaskConcreteBuilder { //implements TaskBuilder {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private IPackager packager;
	
	//@Override
	public Task build(String message, boolean isRedeliver) {

		Task task = build(message);

		// Is the message a redelivery?
		if (isRedeliver) {
			task.setIsRedeliver(isRedeliver);
		}

//		if (task instanceof Deposit) {
//		Deposit deposit = (Deposit) task;
//		deposit.addPackager(packager);	
//	}
//	
//	if (task instanceof Retrieve) {
//		Retrieve retrieve = (Retrieve) task;
//		retrieve.addPackager(packager);	
//	}
		
		return task;

	}

	private Task build(String message) {

		Task concreteTask;

		try {
			ObjectMapper mapper = new ObjectMapper();
			Task commonTask = mapper.readValue(message, Task.class);

			logger.debug("Task commonTask: {}", commonTask);

			Class<?> clazz = Class.forName(commonTask.getTaskClass());
			concreteTask = (Task) (mapper.readValue(message, clazz));
			logger.debug("Task concreteTask: {}", concreteTask);

		} catch (ClassNotFoundException | IOException e) {
			throw new TaskBuilderException("Failed to map message to task", e);
		}
		return concreteTask;
	}

}
