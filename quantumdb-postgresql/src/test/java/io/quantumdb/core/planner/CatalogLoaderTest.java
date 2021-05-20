package io.quantumdb.core.planner;

import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import io.quantumdb.core.backends.PostgresqlDatabase;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Table;
import org.junit.Rule;
import org.junit.Test;

public class CatalogLoaderTest {

	@Rule
	public final PostgresqlDatabase database = new PostgresqlDatabase();

	@Test
	public void ensureWeCanLoadCatalogWithUppercaseCharactersInTablesAndColumns() throws SQLException {
		try (Connection connection = database.createConnection()) {
			try (Statement statement = connection.createStatement()) {
				statement.execute("CREATE TABLE \"Users\" (\"Id\" bigint, \"FirstName\" text, PRIMARY KEY (\"Id\"));");
			}
		}

		try (Connection connection = database.createConnection()) {
			Catalog catalog = CatalogLoader.load(connection, database.getCatalogName());
			Table table = catalog.getTable("Users");

			assertNotNull(table.getColumn("Id"));
			assertNotNull(table.getColumn("FirstName"));
		}
	}

}
