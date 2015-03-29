package io.quantumdb.core.backends;

import java.sql.Connection;
import java.sql.SQLException;

import io.quantumdb.core.versioning.State;

public interface Backend {

	/**
	 * Loads the current state of the database schema and its evolution from the database.
	 *
	 * @return The current state of the database schema.
	 *
	 * @throws SQLException In case the database could not be reached, or queried correctly.
	 */
	State loadState() throws SQLException;

	/**
	 * Persists the current state of the database schema and its evolution to the database.
	 *
	 * @param state The current state of the database schema.
	 *
	 * @throws SQLException In case the database could not be reached, or queried correctly.
	 */
	void persistState(State state) throws SQLException;


	Connection connect() throws SQLException;

	DatabaseMigrator getMigrator();

}
