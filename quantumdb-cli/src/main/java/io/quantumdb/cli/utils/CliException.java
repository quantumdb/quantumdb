package io.quantumdb.cli.utils;

public class CliException extends Exception {

	public CliException(String message) {
		super(message);
	}

	public CliException(String message, Throwable e) {
		super(message, e);
	}

}
