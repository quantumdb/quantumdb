package io.quantumdb.core.schema.operations;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class MergeTableTest {

	@Test
	public void testMergeTable() {
		MergeTable operation = SchemaOperations.mergeTable("users", "users_old", "users_new");

		assertEquals("users", operation.getLeftTableName());
		assertEquals("users_old", operation.getRightTableName());
		assertEquals("users_new", operation.getTargetTableName());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMergeTableThrowsExceptionWhenNullForLeftTableName() {
		SchemaOperations.mergeTable(null, "users_old", "users_new");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMergeTableThrowsExceptionWhenEmptyStringForLeftTableName() {
		SchemaOperations.mergeTable("", "users_old", "users_new");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMergeTableThrowsExceptionWhenNullForRightTableName() {
		SchemaOperations.mergeTable("users", null, "users_new");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMergeTableThrowsExceptionWhenEmptyStringForRightTableName() {
		SchemaOperations.mergeTable("users", "", "users_new");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMergeTableThrowsExceptionWhenNullForTargetTableName() {
		SchemaOperations.mergeTable("users", "users_old", null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMergeTableThrowsExceptionWhenEmptyStringForTargetTableName() {
		SchemaOperations.mergeTable("users", "users_old", "");
	}

}
