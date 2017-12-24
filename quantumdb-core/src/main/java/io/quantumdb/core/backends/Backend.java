package io.quantumdb.core.backends;

import java.sql.Connection;
import java.sql.SQLException;

import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;

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

	/**
	 * Creates a connection to the database.
	 *
	 * @return A database Connection object.
	 *
	 * @throws SQLException In case no connection to the database could be established.
	 */
	Connection connect() throws SQLException;

	/**
	 * @return The DatabaseMigrator implementation for this particular database.
	 */
	DatabaseMigrator getMigrator();

	/**
	 * Determines if this particular backend support the specified JDBC URL.
	 *
	 * @param jdbcUrl The JDBC URL to verify
	 *
	 * @return True if this backend supports the URL, or false otherwise.
	 */
	boolean isJdbcUrlSupported(String jdbcUrl);

	// TODO Move to the migrator.
	int countClientsConnectedToVersion(Version version) throws SQLException;

}
