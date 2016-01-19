package io.quantumdb.core.versioning;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QuantumTables {

	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		Class.forName("org.postgresql.Driver");
		Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/test", "test", "test");
		checkVersion(connection);
	}

	private static final String VERSION_KEY = "meta_info_version";

	private static final List<String> CHANGES = Lists.newArrayList(
			// Creates the "quantumdb_config" table to store persistent configuration and meta info in.
			"CREATE TABLE quantumdb_config (name VARCHAR(255) NOT NULL, value VARCHAR(255) NOT NULL, PRIMARY KEY (name));",

			// Creates the "quantumdb_changesets" table which describes all changesets - named lists of (schema) operations.
			"CREATE TABLE quantumdb_changesets (version_id VARCHAR(10), author VARCHAR(255), created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(), description TEXT, alias VARCHAR(255), PRIMARY KEY (version_id));",

			// Creates the "quantumdb_changelog" table which will store all the individual (schema) operations, and their ordering.
			"CREATE TABLE quantumdb_changelog (version_id VARCHAR(10) NOT NULL, parent_version_id VARCHAR(10), schema_operation TEXT, PRIMARY KEY (version_id));",
			"ALTER TABLE quantumdb_changelog ADD CONSTRAINT quantumdb_changelog_version_id FOREIGN KEY (version_id) REFERENCES quantumdb_changesets (version_id);",
			"ALTER TABLE quantumdb_changelog ADD CONSTRAINT quantumdb_changelog_parent_version_id FOREIGN KEY (parent_version_id) REFERENCES quantumdb_changesets (version_id);",
			"ALTER TABLE quantumdb_changelog ADD CONSTRAINT quantumdb_changelog_no_self_reference CHECK (version_id != parent_version_id);",

			// Creates the "quantumdb_tableids" table which describes which virtual table name, is linked to which physical table name.
			"CREATE TABLE quantumdb_tables (table_id VARCHAR(255) NOT NULL, table_name VARCHAR(255) NOT NULL, PRIMARY KEY (table_id));",

			// Creates the "quantumdb_table_versions" table which describes which (physical) table exists at which version of the changelog.
			"CREATE TABLE quantumdb_table_versions (table_id VARCHAR(255) NOT NULL, version_id VARCHAR(10) NOT NULL, PRIMARY KEY (table_id, version_id));",
			"ALTER TABLE quantumdb_table_versions ADD CONSTRAINT quantumdb_table_versions_table_id FOREIGN KEY (table_id) REFERENCES quantumdb_tables (table_id);",
			"ALTER TABLE quantumdb_table_versions ADD CONSTRAINT quantumdb_table_versions_version_id FOREIGN KEY (version_id) REFERENCES quantumdb_changesets (version_id);",

			// Creates the "quantumdb_table_columns" table which describes which columns exist in the (physical) tables.
			"CREATE TABLE quantumdb_table_columns (id BIGINT NOT NULL, table_id VARCHAR(255) NOT NULL, column_name VARCHAR(255) NOT NULL, PRIMARY KEY (id));",
			"ALTER TABLE quantumdb_table_columns ADD CONSTRAINT quantumdb_table_columns_table_id FOREIGN KEY (table_id) REFERENCES quantumdb_tables (table_id);",
			"ALTER TABLE quantumdb_table_columns ADD CONSTRAINT quantumdb_table_columns_table_id_column_name_uniqueness UNIQUE (table_id, column_name);",

			// Creates the "quantumdb_table_mappings" table which describes how tables are related to each other over time (ie prev/next version of the changelog).
			"CREATE TABLE quantumdb_table_mappings (id BIGINT NOT NULL, source_table_id VARCHAR(255) NOT NULL, target_table_id VARCHAR(255) NOT NULL, PRIMARY KEY (id));",
			"ALTER TABLE quantumdb_table_mappings ADD CONSTRAINT quantumdb_table_mappings_source_table_id FOREIGN KEY (source_table_id) REFERENCES quantumdb_tables (table_id);",
			"ALTER TABLE quantumdb_table_mappings ADD CONSTRAINT quantumdb_table_mappings_target_table_id FOREIGN KEY (target_table_id) REFERENCES quantumdb_tables (table_id);",
			"ALTER TABLE quantumdb_table_mappings ADD CONSTRAINT quantumdb_table_mappings_source_target_uniqueness UNIQUE (source_table_id, target_table_id);",

			// Creates the "quantumdb_column_mappings" table which describes how columns are related to each other over time (ie prev/next version of the changelog).
			"CREATE TABLE quantumdb_column_mappings (id BIGINT NOT NULL, source_column_id BIGINT NOT NULL, target_column_id BIGINT NOT NULL, PRIMARY KEY(id));",
			"ALTER TABLE quantumdb_column_mappings ADD CONSTRAINT quantumdb_column_mappings_source_column_id FOREIGN KEY (source_column_id) REFERENCES quantumdb_table_columns (id);",
			"ALTER TABLE quantumdb_column_mappings ADD CONSTRAINT quantumdb_column_mappings_target_column_id FOREIGN KEY (target_column_id) REFERENCES quantumdb_table_columns (id);",
			"ALTER TABLE quantumdb_column_mappings ADD CONSTRAINT quantumdb_column_mappings_source_target_uniqueness UNIQUE (source_column_id, target_column_id);",

			// Creates the "quantumdb_synchronizers" table which describes which function and trigger is responsible for migrating data between a source and target table.
			"CREATE TABLE quantumdb_synchronizers (id BIGINT NOT NULL, table_mapping_id BIGINT NOT NULL, function_name VARCHAR(255) NOT NULL, trigger_name VARCHAR(255) NOT NULL, PRIMARY KEY(id));",
			"ALTER TABLE quantumdb_synchronizers ADD CONSTRAINT quantumdb_synchronizers_table_mappings_id FOREIGN KEY (table_mapping_id) REFERENCES quantumdb_table_mappings (id);",
			"ALTER TABLE quantumdb_synchronizers ADD CONSTRAINT quantumdb_synchronizers_source_target_uniqueness UNIQUE (table_mapping_id);",
			"ALTER TABLE quantumdb_synchronizers ADD CONSTRAINT quantumdb_synchronizers_unique_function_name UNIQUE (function_name);",
			"ALTER TABLE quantumdb_synchronizers ADD CONSTRAINT quantumdb_synchronizers_unique_trigger_name UNIQUE (trigger_name);",

			// Creates the "quantumdb_synchronizer_columns" table which describes which columns are synchronized by a particular synchronizer.
			"CREATE TABLE quantumdb_synchronizer_columns (synchronizer_id BIGINT NOT NULL, column_mapping_id BIGINT NOT NULL, PRIMARY KEY(synchronizer_id, column_mapping_id));",
			"ALTER TABLE quantumdb_synchronizer_columns ADD CONSTRAINT quantumdb_synchronizer_columns_synchronizer_id FOREIGN KEY (synchronizer_id) REFERENCES quantumdb_synchronizers (id);",
			"ALTER TABLE quantumdb_synchronizer_columns ADD CONSTRAINT quantumdb_synchronizer_columns_column_mapping_id FOREIGN KEY (column_mapping_id) REFERENCES quantumdb_column_mappings (id);"
	);

	public static QuantumTables load(Connection connection) {
		return null;
	}

	private static void checkVersion(Connection connection) throws SQLException {
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
					log.info("Updated meta-info tables to version: {}, took: {} ms", version, (end - start));
				}
				catch (SQLException e) {
					connection.rollback();
					throw e;
				}
			}
		}
	}

	private static void setVersion(Connection connection, int version) throws SQLException {
		String query = "UPDATE quantumdb_config SET value = ? WHERE name = ?;";
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			statement.setString(1, Integer.toString(version));
			statement.setString(2, VERSION_KEY);
			int modified = statement.executeUpdate();
			if (modified > 0) {
				return;
			}
		}

		query = "INSERT INTO quantumdb_config (name, value) VALUES (?, ?);";
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
		String query = "SELECT * FROM information_schema.tables WHERE table_name = 'quantumdb_config' AND table_schema = 'public';";
		try (Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery(query);
			if (!resultSet.next()) {
				return 0;
			}
		}

		query = "SELECT * FROM quantumdb_config WHERE name = ?;";
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
