package io.quantumdb.core.migration.operations;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.bool;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.date;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.integer;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.varchar;
import static org.junit.Assert.assertEquals;

import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.AddColumn;
import io.quantumdb.core.schema.operations.SchemaOperations;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.TableMapping;
import org.junit.Before;
import org.junit.Test;

public class AddColumnMigratorTest {

	private Catalog catalog;
	private Changelog changelog;
	private TableMapping tableMapping;
	private DataMappings dataMappings;
	private AddColumnMigrator migrator;

	@Before
	public void setUp() {
		this.catalog = new Catalog("test-db")
				.addTable(new Table("users")
						.addColumn(new Column("id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
						.addColumn(new Column("name", varchar(255), NOT_NULL)));

		this.changelog = new Changelog();
		this.tableMapping = TableMapping.bootstrap(changelog.getRoot(), catalog);
		this.dataMappings = new DataMappings(tableMapping, catalog);

		this.migrator = new AddColumnMigrator();
	}

	@Test
	public void testExpandForAddingSingleColumn() {
		AddColumn operation = SchemaOperations.addColumn("users", "date_of_birth", date(), "NULL");
		changelog.addChangeSet("Michael de Jong", "Added 'date_of_birth' column to 'users' table.", operation);
		migrator.expand(catalog, tableMapping, dataMappings, changelog.getLastAdded(), operation);

		Table originalTable = catalog.getTable("users");
		Table ghostTable = getGhostTable(originalTable);

		Table expectedGhostTable = new Table(ghostTable.getName())
				.addColumn(new Column("id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("date_of_birth", date(), "NULL"));

		assertEquals(expectedGhostTable, ghostTable);
	}

	@Test
	public void testExpandForAddingMultipleColumns() {
		AddColumn operation1 = SchemaOperations.addColumn("users", "date_of_birth", date(), "NULL");
		changelog.addChangeSet("Michael de Jong", "Added 'date_of_birth' column to 'users' table.", operation1);
		migrator.expand(catalog, tableMapping, dataMappings, changelog.getLastAdded(), operation1);

		AddColumn operation2 = SchemaOperations.addColumn("users", "activated", bool(), "TRUE");
		changelog.addChangeSet("Michael de Jong", "Added 'activated' column to 'users' table.", operation2);
		migrator.expand(catalog, tableMapping, dataMappings, changelog.getLastAdded(), operation2);

		Table originalTable = catalog.getTable("users");
		Table ghostTable = getGhostTable(originalTable);

		Table expectedGhostTable = new Table(ghostTable.getName())
				.addColumn(new Column("id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
				.addColumn(new Column("name", varchar(255), NOT_NULL))
				.addColumn(new Column("date_of_birth", date(), "NULL"))
				.addColumn(new Column("activated", bool(), "TRUE"));

		assertEquals(3, catalog.getTables().size());
		assertEquals(expectedGhostTable, ghostTable);
	}

	private Table getGhostTable(Table table) {
		String tableId = tableMapping.getTableId(changelog.getLastAdded(), table.getName());
		return catalog.getTable(tableId);
	}

}
