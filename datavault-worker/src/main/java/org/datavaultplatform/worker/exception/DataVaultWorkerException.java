package org.datavaultplatform.worker.exception;

import org.datavaultplatform.common.exception.DataVaultException;

public class DataVaultWorkerException extends DataVaultException {

	private static final long serialVersionUID = -317373183265183595L;

	public DataVaultWorkerException() {
		super();
	}

	public DataVaultWorkerException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public DataVaultWorkerException(String message, Throwable cause) {
		super(message, cause);
	}

	public DataVaultWorkerException(String message) {
		super(message);
	}

	public DataVaultWorkerException(Throwable cause) {
		super(cause);
	}



}
