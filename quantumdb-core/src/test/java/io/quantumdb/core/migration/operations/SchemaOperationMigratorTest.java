package io.quantumdb.core.migration.operations;

import static io.quantumdb.core.backends.postgresql.PostgresTypes.bigint;
import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.operations.SchemaOperations.createTable;
import static org.junit.Assert.assertEquals;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.Version;
import org.junit.Before;
import org.junit.Test;

public class SchemaOperationMigratorTest {

	private RefLog refLog;
	private Catalog catalog;
	private Changelog changelog;
	private SchemaOperationsMigrator migrator;

	@Before
	public void setUp() {
		this.catalog = new Catalog("test-db");
		this.changelog = new Changelog();
		this.refLog = RefLog.init(catalog, changelog.getRoot());
		this.migrator = new SchemaOperationsMigrator(catalog, refLog);
	}

	@Test
	public void testAddingNewTable() {
		changelog.addChangeSet("Michael de Jong",
				createTable("users")
						.with("id", bigint(), NOT_NULL, AUTO_INCREMENT, IDENTITY));

		Version current = changelog.getLastAdded();
		migrator.migrate(current, current.getSchemaOperation());

		String tableId = refLog.getTableRef(current, "users").getTableId();

		Table expected = new Table(tableId)
				.addColumn(new Column("id", bigint(), NOT_NULL, AUTO_INCREMENT, IDENTITY));

		assertEquals(expected, catalog.getTable(tableId));
	}

}
