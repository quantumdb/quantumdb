package io.quantumdb.core.migration.operations;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.TestTypes.integer;
import static io.quantumdb.core.schema.definitions.TestTypes.varchar;
import static org.junit.Assert.assertEquals;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.DropColumn;
import io.quantumdb.core.schema.operations.SchemaOperations;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.TableRef;
import org.junit.Before;
import org.junit.Test;

public class DropColumnMigratorTest {

	private RefLog refLog;
	private Catalog catalog;
	private Changelog changelog;
	private DropColumnMigrator migrator;

	@Before
	public void setUp() {
		this.catalog = new Catalog("test-db")
				.addTable(new Table("users")
						.addColumn(new Column("id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
						.addColumn(new Column("name", varchar(255), NOT_NULL)));

		this.changelog = new Changelog();
		this.refLog = RefLog.init(catalog, changelog.getRoot());

		this.migrator = new DropColumnMigrator();
	}

	@Test
	public void testExpandForDroppingSingleColumn() {
		DropColumn operation = SchemaOperations.dropColumn("users", "name");
		changelog.addChangeSet("Michael de Jong", "Dropped 'name' column from 'users' table.", operation);
		migrator.migrate(catalog, refLog, changelog.getLastAdded(), operation);

		Table originalTable = catalog.getTable("users");
		Table ghostTable = getGhostTable(originalTable);

		Table expectedGhostTable = new Table(ghostTable.getName())
				.addColumn(new Column("id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT));

		assertEquals(expectedGhostTable, ghostTable);
	}

	@Test(expected = IllegalStateException.class)
	public void testExpandForDroppingIdentityColumn() {
		DropColumn operation = SchemaOperations.dropColumn("users", "id");
		changelog.addChangeSet("Michael de Jong", "Dropped 'id' column from 'users' table.", operation);
		migrator.migrate(catalog, refLog, changelog.getLastAdded(), operation);
	}

	private Table getGhostTable(Table table) {
		TableRef tableRef = refLog.getTableRef(changelog.getLastAdded(), table.getName());
		String tableId = tableRef.getTableId();
		return catalog.getTable(tableId);
	}

}
