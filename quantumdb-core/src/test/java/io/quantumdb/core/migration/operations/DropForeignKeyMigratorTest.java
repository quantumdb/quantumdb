package io.quantumdb.core.migration.operations;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.TestTypes.integer;
import static io.quantumdb.core.schema.definitions.TestTypes.varchar;
import static io.quantumdb.core.schema.operations.SchemaOperations.dropForeignKey;
import static org.junit.Assert.assertEquals;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.DropForeignKey;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.TableRef;
import org.junit.Before;
import org.junit.Test;

public class DropForeignKeyMigratorTest {

	private RefLog refLog;
	private Catalog catalog;
	private Changelog changelog;
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
		this.refLog = RefLog.init(catalog, changelog.getRoot());

		this.migrator = new DropForeignKeyMigrator();
	}

	@Test
	public void testExpandForDroppingForeignKey() {
		DropForeignKey operation = dropForeignKey("posts", "post_author");
		changelog.addChangeSet("Michael de Jong", "Drop author foreign key from posts table.", operation);
		migrator.migrate(catalog, refLog, changelog.getLastAdded(), operation);

		Table originalTable = catalog.getTable("posts");
		Table ghostTable = getGhostTable(originalTable);

		Table expectedGhostTable = new Table(ghostTable.getName())
				.addColumn(new Column("id", integer(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
				.addColumn(new Column("author", integer(), NOT_NULL));

		assertEquals(expectedGhostTable, ghostTable);
	}

	private Table getGhostTable(Table table) {
		TableRef tableRef = refLog.getTableRef(changelog.getLastAdded(), table.getName());
		String tableId = tableRef.getTableId();
		return catalog.getTable(tableId);
	}

}
