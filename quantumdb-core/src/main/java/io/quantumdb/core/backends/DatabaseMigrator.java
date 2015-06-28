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

	/**
	 * Enters the database schema into a mixed state containing both the from Version as well as the to Version.
	 *
	 * @param state The state of the database.
	 * @param from The current version of the database schema.
	 * @param to The next version of the database schema.
	 * @throws MigrationException In case something prevented the migration.
	 */
	void migrate(State state, Version from, Version to) throws MigrationException;

	/**
	 * Drops the specified version of the database schema.
	 *
	 * @param state The state of the database.
	 * @param version The version of the database schema to drop.
	 * @throws MigrationException In case something prevented the drop.
	 */
	void drop(State state, Version version) throws MigrationException;

}
