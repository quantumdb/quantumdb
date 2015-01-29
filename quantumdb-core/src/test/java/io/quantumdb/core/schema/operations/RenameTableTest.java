package io.quantumdb.core.schema.operations;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class RenameTableTest {

	@Test
	public void testRenameTable() {
		RenameTable operation = SchemaOperations.renameTable("users", "players");

		assertEquals("users", operation.getTableName());
		assertEquals("players", operation.getNewTableName());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testRenameTableThrowsExceptionWhenNullForTableName() {
		SchemaOperations.renameTable(null, "players");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testRenameTableThrowsExceptionWhenEmptyStringForTableName() {
		SchemaOperations.renameTable("", "age");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testRenameTableThrowsExceptionWhenNullForNewTableName() {
		SchemaOperations.renameTable("users", null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testRenameTableThrowsExceptionWhenEmptyStringForNewTableName() {
		SchemaOperations.renameTable("users", "");
	}

}
