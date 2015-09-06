package io.quantumdb.core.migration.operations;

import static io.quantumdb.core.backends.postgresql.PostgresTypes.integer;
import static io.quantumdb.core.backends.postgresql.PostgresTypes.varchar;
import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.operations.SchemaOperations.dropForeignKey;
import static org.junit.Assert.assertEquals;

import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.DropForeignKey;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.TableMapping;
import org.junit.Before;
import org.junit.Test;

public class DropForeignKeyMigratorTest {

	private Catalog catalog;
	private Changelog changelog;
	private TableMapping tableMapping;
	private DataMappings dataMappings;
	private DropForeignKeyMigrator migrator;

	@Before
	public void setUp() {
		this.catalog = new Catalog("test-db")
				.addTable(new Table("users")
						.addColumn(new Column("id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
						.addColumn(new Column("name", varchar(255), NOT_NULL)))
				.addTable(new Table("posts")
						.addColumn(new Column("id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
						.addColumn(new Column("author", integer(), NOT_NULL)));

		Table posts = catalog.getTable("posts");
		Table users = catalog.getTable("users");
		posts.addForeignKey("author")
				.named("post_author")
				.referencing(users, "id");

		this.changelog = new Changelog();
		this.tableMapping = TableMapping.bootstrap(changelog.getRoot(), catalog);
		this.dataMappings = new DataMappings(tableMapping, catalog);

		this.migrator = new DropForeignKeyMigrator();
	}

	@Test
	public void testExpandForDroppingForeignKey() {
		DropForeignKey operation = dropForeignKey("posts", "post_author");
		changelog.addChangeSet("Michael de Jong", "Drop author foreign key from posts table.", operation);
		migrator.migrate(catalog, tableMapping, dataMappings, changelog.getLastAdded(), operation);

		Table originalTable = catalog.getTable("posts");
		Table ghostTable = getGhostTable(originalTable);

		Table expectedGhostTable = new Table(ghostTable.getName())
				.addColumn(new Column("id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
				.addColumn(new Column("author", integer(), NOT_NULL));

		assertEquals(expectedGhostTable, ghostTable);
	}

	private Table getGhostTable(Table table) {
		String tableId = tableMapping.getTableId(changelog.getLastAdded(), table.getName());
		return catalog.getTable(tableId);
	}

}
