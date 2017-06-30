package org.datavaultplatform.worker.queue;

import org.datavaultplatform.common.task.Task;

public interface TaskBuilder {

	Task build(String message, boolean isRedeliver);

}