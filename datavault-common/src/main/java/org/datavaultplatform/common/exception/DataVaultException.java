package org.datavaultplatform.common.exception;

public class DataVaultException extends RuntimeException {

	private static final long serialVersionUID = -317373183265183595L;

	public DataVaultException() {
		super();
	}

	public DataVaultException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public DataVaultException(String message, Throwable cause) {
		super(message, cause);
	}

	public DataVaultException(String message) {
		super(message);
	}

	public DataVaultException(Throwable cause) {
		super(cause);
	}



}
