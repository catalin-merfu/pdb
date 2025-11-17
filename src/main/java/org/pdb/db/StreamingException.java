package org.pdb.db;

/**
 * Exception thrown while performing database queries.
 */
public class StreamingException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	StreamingException() {
		super();
	}

	StreamingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	StreamingException(String message, Throwable cause) {
		super(message, cause);
	}

	StreamingException(String message) {
		super(message);
	}

	StreamingException(Throwable cause) {
		super(cause);
	}

	
}
