package io.quantumdb.core.versioning;

import static io.quantumdb.core.backends.postgresql.PostgresTypes.date;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.text;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.varchar;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

import com.google.common.collect.Sets;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.postgresql.migrator.TableCreator;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Column.Hint;
import io.quantumdb.core.schema.definitions.Table;

class BackendUtils {

	@FunctionalInterface
	interface TransactionalAction<T> {
		T execute() throws SQLException;
	}

	static <T> T inTransaction(Connection connection, TransactionalAction<T> action) throws SQLException {
		boolean autoCommit = connection.getAutoCommit();
		connection.setAutoCommit(false);

		T result;
		try {
			result = action.execute();
			connection.commit();
		}
		catch (SQLException e) {
			connection.rollback();
			throw e;
		}

		connection.setAutoCommit(autoCommit);
		return result;
	}

	static void ensureQuantumDbTablesExist(Backend backend, Connection connection) throws SQLException {
		DatabaseMetaData metaData = connection.getMetaData();
		boolean changelogExists = tableExists(metaData, "quantumdb_changelog");
		boolean changesetsExists = tableExists(metaData, "quantumdb_changesets");
		boolean tableMappingsExists = tableExists(metaData, "quantumdb_tablemappings");
		boolean functionsExists = tableExists(metaData, "quantumdb_functions");
		boolean triggersExists = tableExists(metaData, "quantumdb_triggers");

		Set<Table> tables = Sets.newHashSet();
		Table changelog = new Table("quantumdb_changelog")
				.addColumn(new Column("version_id", varchar(32), Hint.IDENTITY, Hint.NOT_NULL))
				.addColumn(new Column("schema_operation", text()))
				.addColumn(new Column("parent_version_id", varchar(32)));

		Table changesets = new Table("quantumdb_changesets")
				.addColumn(new Column("version_id", varchar(32), Hint.IDENTITY, Hint.NOT_NULL))
				.addColumn(new Column("author", varchar(32), Hint.NOT_NULL))
				.addColumn(new Column("created", date(), Hint.NOT_NULL))
				.addColumn(new Column("description", text()));

		Table tableMappings = new Table("quantumdb_tablemappings")
				.addColumn(new Column("table_name", varchar(255), Hint.IDENTITY, Hint.NOT_NULL))
				.addColumn(new Column("table_id", varchar(255), Hint.NOT_NULL))
				.addColumn(new Column("version_id", varchar(32), Hint.IDENTITY, Hint.NOT_NULL));

		Table functions = new Table("quantumdb_functions")
				.addColumn(new Column("source_table_id", varchar(255), Hint.IDENTITY, Hint.NOT_NULL))
				.addColumn(new Column("target_table_id", varchar(255), Hint.IDENTITY, Hint.NOT_NULL))
				.addColumn(new Column("function_name", varchar(255)));

		Table triggers = new Table("quantumdb_triggers")
				.addColumn(new Column("source_table_id", varchar(255), Hint.IDENTITY, Hint.NOT_NULL))
				.addColumn(new Column("target_table_id", varchar(255), Hint.IDENTITY, Hint.NOT_NULL))
				.addColumn(new Column("trigger_name", varchar(255)));

		changelog.addForeignKey("parent_version_id").referencing(changelog, "version_id");
		changesets.addForeignKey("version_id").referencing(changelog, "version_id");
//		tableMappings.addForeignKey("version_id").referencing(changelog, "version_id");

		if (!changelogExists) {
			tables.add(changelog);
		}
		if (!changesetsExists) {
			tables.add(changesets);
		}
		if (!tableMappingsExists) {
			tables.add(tableMappings);
		}
		if (!functionsExists) {
			tables.add(functions);
		}
		if (!triggersExists) {
			tables.add(triggers);
		}

		if (!tables.isEmpty()) {
			TableCreator tableCreator = backend.getTableCreator();
			tableCreator.create(connection, tables);
		}
	}

	private static boolean tableExists(DatabaseMetaData metaData, String tableName) throws SQLException {
		try (ResultSet tables = metaData.getTables(null, null, tableName, new String[] { "TABLE" })) {
			if (!tables.next()) {
				return false;
			}
		}
		return true;
	}

}
