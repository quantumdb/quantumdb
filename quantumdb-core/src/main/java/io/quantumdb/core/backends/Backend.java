package io.quantumdb.core.backends;

import java.sql.SQLException;
import java.util.Collection;

import io.quantumdb.core.migration.utils.DataMapping;
import io.quantumdb.core.schema.definitions.Table;
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

	/**
	 * Creates the specified Collection of Tables in the database.
	 *
	 * @param tables The Collection of Tables to create in the database. Only foreign keys
	 *               originating from one of these tables will be created.
	 *
	 * @throws SQLException In case the database could not be reached, or queried correctly.
	 */
	void createTables(Collection<Table> tables) throws SQLException;

	/**
	 * Defines which columns map from to which other columns (uni-directional). Some
	 * implementations of this interface may choose to install triggers and functions which
	 * handle synchronizing one table with another this way.
	 *
	 * @param dataMapping The DataMapping object describing which table and its columns map
	 *                    to which other table and that table's columns.
	 *
	 * @throws SQLException In case the database could not be reached, or queried correctly.
	 */
	void installDataMapping(DataMapping dataMapping) throws SQLException;

	/**
	 * Migrates data from one table to another table as defined in the specified DataMapping
	 * object.
	 *
	 * @param dataMapping The DataMapping object describing from which table to which other
	 *                    table to migrate the data.
	 *
	 * @throws SQLException In case the database could not be reached, or queried correctly.
	 */
	void migrateData(DataMapping dataMapping) throws SQLException, InterruptedException;

}
