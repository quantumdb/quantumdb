package io.quantumdb.core.schema.operations;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class CopyTableTest {

	@Test
	public void testCopyTableOperation() {
		CopyTable operation = SchemaOperations.copyTable("users", "players");

		assertEquals("users", operation.getSourceTableName());
		assertEquals("players", operation.getTargetTableName());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatCopyTableOperationThrowsExceptionWhenNullInputForSourceTableName() {
		SchemaOperations.copyTable(null, "players");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatCopyTableOperationThrowsExceptionWhenEmptyStringInputForSourceTableName() {
		SchemaOperations.copyTable("", "players");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatCopyTableOperationThrowsExceptionWhenNullInputForTargetTableName() {
		SchemaOperations.copyTable("users", null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatCopyTableOperationThrowsExceptionWhenEmptyStringInputForTargetTableName() {
		SchemaOperations.copyTable("users", "");
	}

}
