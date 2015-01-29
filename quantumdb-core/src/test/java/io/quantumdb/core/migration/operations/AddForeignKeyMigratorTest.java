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
import io.quantumdb.core.schema.operations.AddForeignKey;
import io.quantumdb.core.schema.operations.SchemaOperations;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.TableMapping;
import org.junit.Before;
import org.junit.Test;

public class AddForeignKeyMigratorTest {

	private Catalog catalog;
	private Changelog changelog;
	private TableMapping tableMapping;
	private DataMappings dataMappings;
	private AddForeignKeyMigrator migrator;

	@Before
	public void setUp() {
		this.catalog = new Catalog("test-db")
				.addTable(new Table("users")
						.addColumn(new Column("id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
						.addColumn(new Column("name", varchar(255), NOT_NULL)))
				.addTable(new Table("posts")
						.addColumn(new Column("id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
						.addColumn(new Column("author", integer(), NOT_NULL)));

		this.changelog = new Changelog();
		this.tableMapping = TableMapping.bootstrap(changelog.getRoot(), catalog);
		this.dataMappings = new DataMappings(tableMapping, catalog);

		this.migrator = new AddForeignKeyMigrator();
	}

	@Test
	public void testExpandForAddingSingleColumn() {
		AddForeignKey operation = SchemaOperations.addForeignKey("posts", "author").referencing("users", "id");
		changelog.addChangeSet("Michael de Jong", "Added 'date_of_birth' column to 'users' table.", operation);
		migrator.expand(catalog, tableMapping, dataMappings, changelog.getLastAdded(), operation);

		Table usersTable = catalog.getTable("users");
		Table originalTable = catalog.getTable("posts");
		Table ghostTable = getGhostTable(originalTable);

		Table expectedGhostTable = new Table(ghostTable.getName())
				.addColumn(new Column("id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
				.addColumn(new Column("author", integer(), NOT_NULL));

		expectedGhostTable.addForeignKey("author").referencing(usersTable, "id");

		assertEquals(expectedGhostTable, ghostTable);
	}

	private Table getGhostTable(Table table) {
		String tableId = tableMapping.getTableId(changelog.getLastAdded(), table.getName());
		return catalog.getTable(tableId);
	}

}
