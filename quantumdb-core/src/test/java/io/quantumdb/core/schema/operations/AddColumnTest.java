package io.quantumdb.core.schema.operations;

import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.TestTypes.varchar;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class AddColumnTest {

	@Test
	public void testAddingColumnWithoutHints() {
		AddColumn operation = SchemaOperations.addColumn("users", "name", varchar(255), NOT_NULL);

		assertEquals("users", operation.getTableName());
		assertEquals(new ColumnDefinition("name", varchar(255), NOT_NULL), operation.getColumnDefinition());
	}

	@Test
	public void testAddingColumnWithDefaultExpression() {
		AddColumn operation = SchemaOperations.addColumn("users", "name", varchar(255), "'unknown'");

		assertEquals("users", operation.getTableName());
		assertEquals(new ColumnDefinition("name", varchar(255), "'unknown'"), operation.getColumnDefinition());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddingColumnWithNullForTableName() {
		SchemaOperations.addColumn(null, "name", varchar(255), NOT_NULL);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddingColumnWithEmptyStringForTableName() {
		SchemaOperations.addColumn("", "name", varchar(255), NOT_NULL);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddingColumnWithNullForColumnName() {
		SchemaOperations.addColumn("users", null, varchar(255), NOT_NULL);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddingColumnWithEmptyStringForColumnName() {
		SchemaOperations.addColumn("users", "", varchar(255), NOT_NULL);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddingColumnWithNullForColumnType() {
		SchemaOperations.addColumn("users", "name", null, NOT_NULL);
	}

}
