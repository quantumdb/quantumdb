package io.quantumdb.core.schema.operations;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.GenericColumnTypes.int8;
import static io.quantumdb.core.schema.definitions.GenericColumnTypes.varchar;
import static org.junit.Assert.assertEquals;

import java.util.List;

import com.google.common.collect.Lists;
import io.quantumdb.core.schema.definitions.Column;
import org.junit.Test;

public class CreateTableTest {

	@Test
	public void testCreatingTableWithDefaultExpression() {
		CreateTable operation = SchemaOperations.createTable("addresses")
				.with("id", int8(), "'1'", IDENTITY, NOT_NULL);

		List<Column> expectedColumns = Lists.newArrayList(new Column("id", int8(), "'1'", IDENTITY, NOT_NULL));

		assertEquals("addresses", operation.getTableName());
		assertEquals(expectedColumns, operation.getColumns());
	}

	@Test
	public void testCreatingTableWithMultipleColumns() {
		CreateTable operation = SchemaOperations.createTable("addresses")
				.with("id", int8(), IDENTITY, AUTO_INCREMENT, NOT_NULL)
				.with("street", varchar(255), NOT_NULL)
				.with("street_number", varchar(10), NOT_NULL)
				.with("city", varchar(255), NOT_NULL)
				.with("postal_code", varchar(10), NOT_NULL)
				.with("country", varchar(255), NOT_NULL);

		List<Column> expectedColumns = Lists.newArrayList(
				new Column("id", int8(), IDENTITY, AUTO_INCREMENT, NOT_NULL),
				new Column("street", varchar(255), NOT_NULL),
				new Column("street_number", varchar(10), NOT_NULL),
				new Column("city", varchar(255), NOT_NULL),
				new Column("postal_code", varchar(10), NOT_NULL),
				new Column("country", varchar(255), NOT_NULL)
		);

		assertEquals("addresses", operation.getTableName());
		assertEquals(expectedColumns, operation.getColumns());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCreatingTableWithNullForTableName() {
		SchemaOperations.createTable(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCreatingTableWithEmptyStringForTableName() {
		SchemaOperations.createTable("");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCreatingTableWithNullForColumnName() {
		SchemaOperations.createTable("addresses")
				.with(null, int8(), IDENTITY, AUTO_INCREMENT, NOT_NULL);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCreatingTableWithEmptyStringForColumnName() {
		SchemaOperations.createTable("addresses")
				.with("", int8(), IDENTITY, AUTO_INCREMENT, NOT_NULL);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCreatingTableWithNullForColumnType() {
		SchemaOperations.createTable("addresses")
				.with("id", null, IDENTITY, AUTO_INCREMENT, NOT_NULL);
	}

}