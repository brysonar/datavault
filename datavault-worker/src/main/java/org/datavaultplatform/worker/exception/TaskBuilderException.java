package org.datavaultplatform.worker.exception;

public class TaskBuilderException extends DataVaultWorkerException {

	private static final long serialVersionUID = 4959125775218295675L;

	public TaskBuilderException(String message) {
		super(message);
	}

	public TaskBuilderException(String message, Throwable cause) {
		super(message, cause);
	}

}
