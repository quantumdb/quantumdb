package io.quantumdb.core.schema.definitions;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.GenericColumnTypes.int8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Strings;
import org.junit.Test;

public class CatalogTest {

	@Test
	public void testCreatingCatalog() {
		Catalog catalog = new Catalog("public");

		assertEquals("public", catalog.getName());
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
		new Catalog("public").addTable(null);
	}

	@Test
	public void testAddingTableToCatalog() {
		new Catalog("public").addTable(new Table("users")
				.addColumn(new Column("id", int8(), IDENTITY, AUTO_INCREMENT)));
	}

	@Test
	public void testThatContainsTableMethodReturnsTrueWhenTableExists() {
		Table table = new Table("users")
				.addColumn(new Column("id", int8(), IDENTITY, AUTO_INCREMENT));

		Catalog catalog = new Catalog("public")
				.addTable(table);

		assertTrue(catalog.containsTable("users"));
	}

	@Test
	public void testThatContainsTableMethodReturnsFalseWhenTableDoesNotExist() {
		Catalog catalog = new Catalog("public");

		assertFalse(catalog.containsTable("users"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatContainsTableMethodThrowsExceptionOnNullInput() {
		Catalog catalog = new Catalog("public");
		catalog.containsTable(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatContainsTableMethodThrowsExceptionOnEmptyStringInput() {
		Catalog catalog = new Catalog("public");
		catalog.containsTable("");
	}

	@Test
	public void testThatGetTableMethodReturnsTableWhenItExists() {
		Table table = new Table("users")
				.addColumn(new Column("id", int8(), IDENTITY, AUTO_INCREMENT));

		Catalog catalog = new Catalog("public")
				.addTable(table);

		assertEquals(table, catalog.getTable("users"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatGetTableMethodThrowsExceptionWhenTableDoesNotExist() {
		Catalog catalog = new Catalog("public");
		catalog.getTable("users");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatGetTableMethodThrowsExceptionOnNullInput() {
		Catalog catalog = new Catalog("public");
		catalog.getTable(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatGetTableMethodThrowsExceptionOnEmptyStringInput() {
		Catalog catalog = new Catalog("public");
		catalog.getTable("");
	}

	@Test
	public void testThatRemoveTableMethodRemovesTableWhenItExists() {
		Table table = new Table("users")
				.addColumn(new Column("id", int8(), IDENTITY, AUTO_INCREMENT));

		Catalog catalog = new Catalog("public")
				.addTable(table);

		Table removedTable = catalog.removeTable("users");
		assertEquals(table, removedTable);
		assertFalse(catalog.containsTable("users"));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatRemoveTableMethodThrowsExceptionWhenTableDoesNotExist() {
		Catalog catalog = new Catalog("public");
		catalog.removeTable("users");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatRemoveTableMethodThrowsExceptionOnNullInput() {
		Catalog catalog = new Catalog("public");
		catalog.removeTable(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatRemoveTableMethodThrowsExceptionOnEmptyStringInput() {
		Catalog catalog = new Catalog("public");
		catalog.removeTable("");
	}

	@Test
	public void testThatCopyTableReturnCopy() {
		Table table = new Table("users")
				.addColumn(new Column("id", int8(), IDENTITY, AUTO_INCREMENT));

		Catalog catalog = new Catalog("public")
				.addTable(table);

		Catalog copy = catalog.copy();

		assertEquals(catalog, copy);
		assertFalse(catalog == copy);
	}

	@Test
	public void toStringReturnsSomething() {
		Table table = new Table("users")
				.addColumn(new Column("id", int8(), IDENTITY, AUTO_INCREMENT));

		Catalog catalog = new Catalog("public")
				.addTable(table);

		assertFalse(Strings.isNullOrEmpty(catalog.toString()));
	}

}
