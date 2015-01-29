package io.quantumdb.core.schema.operations;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.PostgresTypes.bigint;
import static io.quantumdb.core.schema.definitions.PostgresTypes.varchar;
import static org.junit.Assert.assertEquals;

import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Test;

public class CreateTableTest {

	@Test
	public void testCreatingTableWithDefaultExpression() {
		CreateTable operation = SchemaOperations.createTable("addresses")
				.with("id", bigint(), "'1'", IDENTITY, NOT_NULL);

		List<ColumnDefinition> expectedColumns = Lists.newArrayList(
				new ColumnDefinition("id", bigint(), "'1'", IDENTITY, NOT_NULL));

		assertEquals("addresses", operation.getTableName());
		assertEquals(expectedColumns, operation.getColumns());
	}

	@Test
	public void testCreatingTableWithMultipleColumns() {
		CreateTable operation = SchemaOperations.createTable("addresses")
				.with("id", bigint(), IDENTITY, AUTO_INCREMENT, NOT_NULL)
				.with("street", varchar(255), NOT_NULL)
				.with("street_number", varchar(10), NOT_NULL)
				.with("city", varchar(255), NOT_NULL)
				.with("postal_code", varchar(10), NOT_NULL)
				.with("country", varchar(255), NOT_NULL);

		List<ColumnDefinition> expectedColumns = Lists.newArrayList(
				new ColumnDefinition("id", bigint(), IDENTITY, AUTO_INCREMENT, NOT_NULL),
				new ColumnDefinition("street", varchar(255), NOT_NULL),
				new ColumnDefinition("street_number", varchar(10), NOT_NULL),
				new ColumnDefinition("city", varchar(255), NOT_NULL),
				new ColumnDefinition("postal_code", varchar(10), NOT_NULL),
				new ColumnDefinition("country", varchar(255), NOT_NULL)
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
				.with(null, bigint(), IDENTITY, AUTO_INCREMENT, NOT_NULL);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCreatingTableWithEmptyStringForColumnName() {
		SchemaOperations.createTable("addresses")
				.with("", bigint(), IDENTITY, AUTO_INCREMENT, NOT_NULL);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCreatingTableWithNullForColumnType() {
		SchemaOperations.createTable("addresses")
				.with("id", null, IDENTITY, AUTO_INCREMENT, NOT_NULL);
	}

}
