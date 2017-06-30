package org.datavaultplatform.worker.queue;

import org.datavaultplatform.common.task.Task;
import org.datavaultplatform.worker.exception.TaskBuilderException;
import org.datavaultplatform.worker.tasks.Deposit;
import org.datavaultplatform.worker.tasks.ITaskAction;
import org.datavaultplatform.worker.tasks.Retrieve;
import org.springframework.beans.factory.annotation.Autowired;

public class TaskFactory {

	private static final String retrieveTask = "org.datavaultplatform.worker.tasks.Retrieve";
	private static final String depositTask = "org.datavaultplatform.worker.tasks.Deposit";
	
	@Autowired
	private Retrieve retrieve;
	
	@Autowired
	private Deposit deposit;
	
	
	public ITaskAction getTaskAction(Task task) {
		return getTaskAction(task.getTaskClass());
	}

	public ITaskAction getTaskAction(String taskType) {
		
		ITaskAction concreteTask = null;
		if (retrieveTask.equals(taskType)) {
			concreteTask = retrieve;
		} else if (depositTask.equals(taskType)) {
			concreteTask = deposit;
		} else {
			throw new TaskBuilderException("Task type " + taskType + "Not supported");
		}

		return concreteTask;
	}
}
