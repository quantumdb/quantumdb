package io.quantumdb.core.backends;

import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;

public interface DatabaseMigrator {

	public static class MigrationException extends Exception {
		public MigrationException(Throwable e) {
			super(e);
		}

		public MigrationException(String message) {
			super(message);
		}

		public MigrationException(String message, Throwable e) {
			super(message, e);
		}
	}

	void migrate(State state, Version from, Version to) throws MigrationException;

}
