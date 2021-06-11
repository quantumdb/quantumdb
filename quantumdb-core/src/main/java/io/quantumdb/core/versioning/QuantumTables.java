package io.quantumdb.core.versioning;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QuantumTables {

	private static final String VERSION_KEY = "meta_info_version";

	private static final List<String> CHANGES = Lists.newArrayList(
			// Creates the "config" table to store persistent configuration and meta info in.
			"CREATE TABLE quantumdb.config (name VARCHAR(255) NOT NULL, value VARCHAR(255) NOT NULL, PRIMARY KEY (name));",

			// Creates the "changelog" table which will store all the individual (schema) operations, and their ordering.
			"CREATE TABLE quantumdb.changelog (version_id VARCHAR(10) NOT NULL, parent_version_id VARCHAR(10), operation_type VARCHAR(16), operation TEXT, PRIMARY KEY (version_id));",
			"ALTER TABLE quantumdb.changelog ADD CONSTRAINT changelog_parent_version_id FOREIGN KEY (parent_version_id) REFERENCES quantumdb.changelog (version_id) ON DELETE CASCADE;",
			"ALTER TABLE quantumdb.changelog ADD CONSTRAINT changelog_no_self_reference CHECK (version_id != parent_version_id);",

			// Creates the "changesets" table which describes all changesets - named lists of (schema) operations.
			"CREATE TABLE quantumdb.changesets (id VARCHAR(255), version_id VARCHAR(10), author VARCHAR(255), created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(), description TEXT, alias VARCHAR(255), PRIMARY KEY (id));",
			"ALTER TABLE quantumdb.changesets ADD CONSTRAINT changesets_version_id_unique UNIQUE (version_id);",
			"ALTER TABLE quantumdb.changesets ADD CONSTRAINT changesets_version_id FOREIGN KEY (version_id) REFERENCES quantumdb.changelog (version_id) ON DELETE CASCADE;",

			// Creates the "refs" table which describes table ids exist.
			"CREATE TABLE quantumdb.refs (ref_id VARCHAR(255) NOT NULL, PRIMARY KEY (ref_id));",

			// Creates the "ref_versions" table which describes which (physical) table exists at which version of the changelog.
			"CREATE TABLE quantumdb.ref_versions (ref_id VARCHAR(255) NOT NULL, version_id VARCHAR(10) NOT NULL, table_name VARCHAR(255) NOT NULL, PRIMARY KEY (ref_id, version_id));",
			"ALTER TABLE quantumdb.ref_versions ADD CONSTRAINT ref_versions_ref_id FOREIGN KEY (ref_id) REFERENCES quantumdb.refs (ref_id) ON DELETE CASCADE;",
			"ALTER TABLE quantumdb.ref_versions ADD CONSTRAINT ref_versions_version_id FOREIGN KEY (version_id) REFERENCES quantumdb.changelog (version_id) ON DELETE CASCADE;",

			// Creates the "table_columns" table which describes which columns exist in the (physical) tables.
			"CREATE SEQUENCE quantumdb.table_columns_id;",
			"CREATE TABLE quantumdb.table_columns (id BIGINT NOT NULL DEFAULT NEXTVAL('quantumdb.table_columns_id'), ref_id VARCHAR(255) NOT NULL, column_name VARCHAR(255) NOT NULL, PRIMARY KEY (id));",
			"ALTER TABLE quantumdb.table_columns ADD CONSTRAINT table_columns_ref_id FOREIGN KEY (ref_id) REFERENCES quantumdb.refs (ref_id) ON DELETE CASCADE;",
			"ALTER TABLE quantumdb.table_columns ADD CONSTRAINT table_columns_ref_id_column_name_uniqueness UNIQUE (ref_id, column_name);",

			// Creates the "column_mappings" table which describes how columns are related to each other over time (ie prev/next version of the changelog).
			"CREATE SEQUENCE quantumdb.column_mappings_id;",
			"CREATE TABLE quantumdb.column_mappings (id BIGINT NOT NULL DEFAULT NEXTVAL('quantumdb.column_mappings_id'), source_column_id BIGINT NOT NULL, target_column_id BIGINT NOT NULL, PRIMARY KEY(id));",
			"ALTER TABLE quantumdb.column_mappings ADD CONSTRAINT column_mappings_source_column_id FOREIGN KEY (source_column_id) REFERENCES quantumdb.table_columns (id) ON DELETE CASCADE;",
			"ALTER TABLE quantumdb.column_mappings ADD CONSTRAINT column_mappings_target_column_id FOREIGN KEY (target_column_id) REFERENCES quantumdb.table_columns (id) ON DELETE CASCADE;",
			"ALTER TABLE quantumdb.column_mappings ADD CONSTRAINT column_mappings_source_target_uniqueness UNIQUE (source_column_id, target_column_id);",

			// Creates the "synchronizers" table which describes which function and trigger is responsible for migrating data between a source and target table.
			"CREATE SEQUENCE quantumdb.synchronizers_id;",
			"CREATE TABLE quantumdb.synchronizers (id BIGINT NOT NULL DEFAULT NEXTVAL('quantumdb.synchronizers_id'), source_ref_id VARCHAR(255) NOT NULL, target_ref_id VARCHAR(255) NOT NULL, function_name VARCHAR(255) NOT NULL, trigger_name VARCHAR(255) NOT NULL, PRIMARY KEY(id));",
			"ALTER TABLE quantumdb.synchronizers ADD CONSTRAINT synchronizers_source_ref_id FOREIGN KEY (source_ref_id) REFERENCES quantumdb.refs (ref_id) ON DELETE CASCADE;",
			"ALTER TABLE quantumdb.synchronizers ADD CONSTRAINT synchronizers_target_ref_id FOREIGN KEY (target_ref_id) REFERENCES quantumdb.refs (ref_id) ON DELETE CASCADE;",
			"ALTER TABLE quantumdb.synchronizers ADD CONSTRAINT synchronizers_source_target_uniqueness UNIQUE (source_ref_id, target_ref_id);",
			"ALTER TABLE quantumdb.synchronizers ADD CONSTRAINT synchronizers_unique_function_name UNIQUE (function_name);",
			"ALTER TABLE quantumdb.synchronizers ADD CONSTRAINT synchronizers_unique_trigger_name UNIQUE (trigger_name);",

			// Creates the "synchronizer_columns" table which describes which columns are synchronized by a particular synchronizer.
			"CREATE SEQUENCE quantumdb.synchronizer_columns_id;",
			"CREATE TABLE quantumdb.synchronizer_columns (synchronizer_id BIGINT NOT NULL DEFAULT NEXTVAL('quantumdb.synchronizer_columns_id'), column_mapping_id BIGINT NOT NULL, PRIMARY KEY(synchronizer_id, column_mapping_id));",
			"ALTER TABLE quantumdb.synchronizer_columns ADD CONSTRAINT synchronizer_columns_synchronizer_id FOREIGN KEY (synchronizer_id) REFERENCES quantumdb.synchronizers (id) ON DELETE CASCADE;",
			"ALTER TABLE quantumdb.synchronizer_columns ADD CONSTRAINT synchronizer_columns_column_mapping_id FOREIGN KEY (column_mapping_id) REFERENCES quantumdb.column_mappings (id) ON DELETE CASCADE;",

			// Creates the "active_versions" table which describes which versions are active at this time.
			"CREATE TABLE quantumdb.active_versions (version_id VARCHAR(10), PRIMARY KEY (version_id));",
			"ALTER TABLE quantumdb.active_versions ADD CONSTRAINT active_versions_version_id FOREIGN KEY (version_id) REFERENCES quantumdb.changelog (version_id) ON DELETE CASCADE;"
	);

	public static int prepare(Connection connection) throws SQLException {
		connection.setAutoCommit(false);
		int version = getVersion(connection);

		if (version == CHANGES.size()) {
			log.info("Meta-info tables are up to date!");
		}
		else {
			log.info("Current meta-info tables are at version: {}, let's upgrade to version: {}", version, CHANGES.size());
			while (version < CHANGES.size()) {
				try {
					long start = System.currentTimeMillis();
					execute(connection, CHANGES.get(version));
					long end = System.currentTimeMillis();

					version++;
					setVersion(connection, version);
					connection.commit();
					log.debug("Updated meta-info tables to version: {}, took: {} ms", version, (end - start));
				}
				catch (SQLException e) {
					connection.rollback();
					throw e;
				}
			}
		}
		return version;
	}

	public static void dropEverything(Connection connection, String... otherSchemasToDrop) throws SQLException {
		connection.setAutoCommit(false);

		List<String> schemasToDrop = Lists.newArrayList(otherSchemasToDrop);
		schemasToDrop.add(0, "quantumdb");

		try {
			for (String schema : schemasToDrop) {
				Statement statement = connection.createStatement();
				statement.executeUpdate("DROP SCHEMA " + schema + " CASCADE;");
				statement.executeUpdate("CREATE SCHEMA " + schema + ";");
				statement.executeUpdate("ALTER SCHEMA " + schema + " OWNER TO " + connection.getMetaData().getUserName() + ";");
				statement.close();
			}
			connection.commit();
		}
		catch (SQLException e) {
			connection.rollback();
			throw e;
		}
	}

	private static void setVersion(Connection connection, int version) throws SQLException {
		String query = "UPDATE quantumdb.config SET value = ? WHERE name = ?;";
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, Integer.toString(version));
			statement.setString(2, VERSION_KEY);
			int modified = statement.executeUpdate();
			if (modified > 0) {
				return;
			}
		}

		query = "INSERT INTO quantumdb.config (name, value) VALUES (?, ?);";
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, VERSION_KEY);
			statement.setString(2, Integer.toString(version));
			int modified = statement.executeUpdate();
			if (modified == 0) {
				throw new RuntimeException("Could not set/update '" + VERSION_KEY + "' to: " + version);
			}
		}
	}

	private static void execute(Connection connection, String query) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			statement.execute(query);
		}
		catch (SQLException e) {
			log.error("Exception happened while executing: {}", query);
			throw e;
		}
	}

	private static int getVersion(Connection connection) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			statement.executeUpdate("CREATE SCHEMA IF NOT EXISTS quantumdb;");
		}

		String query = "SELECT * FROM information_schema.tables WHERE table_name = 'config' AND table_schema = 'quantumdb';";
		try (Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery(query);
			if (!resultSet.next()) {
				return 0;
			}
		}

		query = "SELECT * FROM quantumdb.config WHERE name = ?;";
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, VERSION_KEY);
			ResultSet resultSet = statement.executeQuery();
			if (resultSet.next()) {
				return Integer.parseInt(resultSet.getString("value"));
			}
		}

		return 0;
	}

}
