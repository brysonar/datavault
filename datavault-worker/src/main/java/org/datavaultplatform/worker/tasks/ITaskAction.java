package org.datavaultplatform.worker.tasks;

import org.datavaultplatform.common.task.Context;
import org.datavaultplatform.common.task.Task;

public interface ITaskAction {

    void performAction(Context context, Task task);
}
