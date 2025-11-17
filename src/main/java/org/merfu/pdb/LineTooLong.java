package org.merfu.pdb;

/**
 * Exception is thrown when a line in an input file is longer than the internal reading buffer
 */
class LineTooLong extends RuntimeException {

	private static final long serialVersionUID = 1L;

	LineTooLong() {
	}

	LineTooLong(String message) {
		super(message);
	}

	LineTooLong(Throwable cause) {
		super(cause);
	}

	LineTooLong(String message, Throwable cause) {
		super(message, cause);
	}

	LineTooLong(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
