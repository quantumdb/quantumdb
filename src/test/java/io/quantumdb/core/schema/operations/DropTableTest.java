package io.quantumdb.core.schema.operations;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class DropTableTest {

	@Test
	public void testDropTable() {
		DropTable operation = SchemaOperations.dropTable("users");

		assertEquals("users", operation.getTableName());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDropTableThrowsExceptionWhenNullForTableName() {
		SchemaOperations.dropTable(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDropTableThrowsExceptionWhenEmptyStringForTableName() {
		SchemaOperations.dropTable("");
	}

}
