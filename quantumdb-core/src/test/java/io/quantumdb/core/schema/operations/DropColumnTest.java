package io.quantumdb.core.schema.operations;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class DropColumnTest {

	@Test
	public void testDropColumn() {
		DropColumn operation = SchemaOperations.dropColumn("users", "age");

		assertEquals("users", operation.getTableName());
		assertEquals("age", operation.getColumnName());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDropColumnThrowsExceptionWhenNullForTableName() {
		SchemaOperations.dropColumn(null, "age");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDropColumnThrowsExceptionWhenEmptyStringForTableName() {
		SchemaOperations.dropColumn("", "age");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDropColumnThrowsExceptionWhenNullForColumnName() {
		SchemaOperations.dropColumn("users", null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDropColumnThrowsExceptionWhenEmptyStringForColumnName() {
		SchemaOperations.dropColumn("users", "");
	}

}
