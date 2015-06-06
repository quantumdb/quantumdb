package io.quantumdb.core.migration.operations;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.integer;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.varchar;
import static org.junit.Assert.assertEquals;

import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.DropColumn;
import io.quantumdb.core.schema.operations.SchemaOperations;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.TableMapping;
import org.junit.Before;
import org.junit.Test;

public class DropColumnMigratorTest {

	private Catalog catalog;
	private Changelog changelog;
	private TableMapping tableMapping;
	private DataMappings dataMappings;
	private DropColumnMigrator migrator;

	@Before
	public void setUp() {
		this.catalog = new Catalog("test-db")
				.addTable(new Table("users")
						.addColumn(new Column("id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
						.addColumn(new Column("name", varchar(255), NOT_NULL)));

		this.changelog = new Changelog();
		this.tableMapping = TableMapping.bootstrap(changelog.getRoot(), catalog);
		this.dataMappings = new DataMappings(tableMapping, catalog);

		this.migrator = new DropColumnMigrator();
	}

	@Test
	public void testExpandForDroppingSingleColumn() {
		DropColumn operation = SchemaOperations.dropColumn("users", "name");
		changelog.addChangeSet("Michael de Jong", "Dropped 'name' column from 'users' table.", operation);
		migrator.migrate(catalog, tableMapping, dataMappings, changelog.getLastAdded(), operation);

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
		migrator.migrate(catalog, tableMapping, dataMappings, changelog.getLastAdded(), operation);
	}

	private Table getGhostTable(Table table) {
		String tableId = tableMapping.getTableId(changelog.getLastAdded(), table.getName());
		return catalog.getTable(tableId);
	}

}
