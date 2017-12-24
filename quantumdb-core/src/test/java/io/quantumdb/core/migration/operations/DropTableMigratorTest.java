package io.quantumdb.core.migration.operations;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.TestTypes.integer;
import static io.quantumdb.core.schema.definitions.TestTypes.varchar;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.DropTable;
import io.quantumdb.core.schema.operations.SchemaOperations;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.RefLog;
import org.junit.Before;
import org.junit.Test;

public class DropTableMigratorTest {

	private RefLog refLog;
	private Catalog catalog;
	private Changelog changelog;
	private DropTableMigrator migrator;

	@Before
	public void setUp() {
		this.catalog = new Catalog("test-db")
				.addTable(new Table("users")
						.addColumn(new Column("id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
						.addColumn(new Column("name", varchar(255), NOT_NULL)));

		this.changelog = new Changelog();
		this.refLog = RefLog.init(catalog, changelog.getRoot());

		this.migrator = new DropTableMigrator();
	}

	@Test
	public void testExpandForDroppingTable() {
		DropTable operation = SchemaOperations.dropTable("users");
		changelog.addChangeSet("Michael de Jong", "Dropped 'users' table.", operation);
		migrator.migrate(catalog, refLog, changelog.getLastAdded(), operation);

		assertEquals(1, catalog.getTables().size());
		assertFalse(refLog.getTableRefs(changelog.getLastAdded()).stream()
				.anyMatch(tableRef -> tableRef.getName().equals("users")));
	}

}
