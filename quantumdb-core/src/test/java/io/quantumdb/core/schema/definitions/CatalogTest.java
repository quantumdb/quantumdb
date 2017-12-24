package io.quantumdb.core.schema.definitions;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.TestTypes.bigint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Strings;
import org.junit.Test;

public class CatalogTest {

	@Test
	public void testCreatingCatalog() {
		Catalog catalog = new Catalog("test-db");

		assertEquals("test-db", catalog.getName());
		assertTrue(catalog.getTables().isEmpty());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCreatingCatalogWithNullForCatalogName() {
		new Catalog(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCreatingCatalogWithEmptyStringForCatalogName() {
		new Catalog("");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddingNullTableToCatalog() {
		new Catalog("test-db").addTable(null);
	}

	@Test
	public void testAddingTableToCatalog() {
		Table table = new Table("users")
				.addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT));

		Catalog catalog = new Catalog("test-db")
				.addTable(table);

		assertEquals(catalog, table.getParent());
	}

	@Test
	public void testThatContainsTableMethodReturnsTrueWhenTableExists() {
		Table table = new Table("users")
				.addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT));

		Catalog catalog = new Catalog("test-db")
				.addTable(table);

		assertTrue(catalog.containsTable("users"));
	}

	@Test
	public void testThatContainsTableMethodReturnsFalseWhenTableDoesNotExist() {
		Catalog catalog = new Catalog("test-db");

		assertFalse(catalog.containsTable("users"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatContainsTableMethodThrowsExceptionOnNullInput() {
		Catalog catalog = new Catalog("test-db");
		catalog.containsTable(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatContainsTableMethodThrowsExceptionOnEmptyStringInput() {
		Catalog catalog = new Catalog("test-db");
		catalog.containsTable("");
	}

	@Test
	public void testThatGetTableMethodReturnsTableWhenItExists() {
		Table table = new Table("users")
				.addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT));

		Catalog catalog = new Catalog("test-db")
				.addTable(table);

		assertEquals(table, catalog.getTable("users"));
	}

	@Test(expected = IllegalStateException.class)
	public void testThatGetTableMethodThrowsExceptionWhenTableDoesNotExist() {
		Catalog catalog = new Catalog("test-db");
		catalog.getTable("users");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatGetTableMethodThrowsExceptionOnNullInput() {
		Catalog catalog = new Catalog("test-db");
		catalog.getTable(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatGetTableMethodThrowsExceptionOnEmptyStringInput() {
		Catalog catalog = new Catalog("test-db");
		catalog.getTable("");
	}

	@Test
	public void testThatRemoveTableMethodRemovesTableWhenItExists() {
		Table table = new Table("users")
				.addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT));

		Catalog catalog = new Catalog("test-db")
				.addTable(table);

		Table removedTable = catalog.removeTable("users");
		assertEquals(table, removedTable);
		assertFalse(catalog.containsTable("users"));
	}

	@Test(expected = IllegalStateException.class)
	public void testThatRemoveTableMethodThrowsExceptionWhenTableDoesNotExist() {
		Catalog catalog = new Catalog("test-db");
		catalog.removeTable("users");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatRemoveTableMethodThrowsExceptionOnNullInput() {
		Catalog catalog = new Catalog("test-db");
		catalog.removeTable(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatRemoveTableMethodThrowsExceptionOnEmptyStringInput() {
		Catalog catalog = new Catalog("test-db");
		catalog.removeTable("");
	}

	@Test
	public void testRemovingTableDropsOutgoingForeignKeys() {
		Catalog catalog = new Catalog("test-db");
		Table users = new Table("users")
				.addColumn(new Column("id", bigint(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
				.addColumn(new Column("address_id", bigint(), NOT_NULL));

		Table addresses = new Table("addresses")
				.addColumn(new Column("id", bigint(), IDENTITY, NOT_NULL, AUTO_INCREMENT));

		catalog.addTable(users);
		catalog.addTable(addresses);

		users.addForeignKey("address_id")
				.referencing(addresses, "id");

		catalog.removeTable("users");

		assertTrue(addresses.getColumn("id").getIncomingForeignKeys().isEmpty());
		assertTrue(addresses.getForeignKeys().isEmpty());
	}

	@Test(expected = IllegalStateException.class)
	public void testRemovingTableThrowsExceptionWhenIncomingForeignKeysExist() {
		Catalog catalog = new Catalog("test-db");
		Table users = new Table("users")
				.addColumn(new Column("id", bigint(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
				.addColumn(new Column("address_id", bigint(), NOT_NULL));

		Table addresses = new Table("addresses")
				.addColumn(new Column("id", bigint(), IDENTITY, NOT_NULL, AUTO_INCREMENT));

		catalog.addTable(users);
		catalog.addTable(addresses);

		users.addForeignKey("address_id")
				.referencing(addresses, "id");

		catalog.removeTable("addresses");
	}

	@Test
	public void testThatRenamingTableIsReflectedInCatalog() {
		Table table = new Table("users")
				.addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT));

		Catalog catalog = new Catalog("test-db")
				.addTable(table);

		table.rename("players");

		assertFalse(catalog.containsTable("users"));
		assertTrue(catalog.containsTable("players"));
		assertEquals(table, catalog.getTable("players"));
	}

	@Test(expected = IllegalStateException.class)
	public void testThatRenamingTableThrowsExceptionWhenNameIsAlreadyTaken() {
		Table usersTable = new Table("users")
				.addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT));

		Table playersTable = new Table("players")
				.addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT));

		new Catalog("test-db")
				.addTable(usersTable)
				.addTable(playersTable);

		usersTable.rename("players");
	}

	@Test
	public void testThatCopyMethodReturnsCopy() {
		Table table = new Table("users")
				.addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT));

		Catalog catalog = new Catalog("test-db")
				.addTable(table);

		Catalog copy = catalog.copy();

		assertEquals(catalog, copy);
		assertFalse(catalog == copy);
	}

	@Test
	public void toStringReturnsSomething() {
		Table table = new Table("users")
				.addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT));

		Catalog catalog = new Catalog("test-db")
				.addTable(table);

		assertFalse(Strings.isNullOrEmpty(catalog.toString()));
	}

}
