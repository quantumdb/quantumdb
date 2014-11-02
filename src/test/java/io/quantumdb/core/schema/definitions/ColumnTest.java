package io.quantumdb.core.schema.definitions;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.GenericColumnTypes.int8;
import static io.quantumdb.core.schema.definitions.GenericColumnTypes.varchar;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Strings;
import org.junit.Test;

public class ColumnTest {

	@Test
	public void testCreatingColumn() {
		Column column = new Column("id", int8());

		assertEquals("id", column.getName());
		assertEquals(int8(), column.getType());
		assertEquals(null, column.getDefaultValueExpression());
	}

	@Test
	public void testCreatingIdentityColumn() {
		Column column = new Column("id", int8(), IDENTITY);

		assertTrue(column.isIdentity());
	}

	@Test
	public void testCreatingAutoIncrementColumn() {
		Column column = new Column("id", int8(), AUTO_INCREMENT);

		assertTrue(column.isAutoIncrement());
	}

	@Test
	public void testCreatingNonNullableColumn() {
		Column column = new Column("id", int8(), NOT_NULL);

		assertTrue(column.isNotNull());
	}

	@Test
	public void testCreatingColumnWithDefaultExpression() {
		Column column = new Column("id", varchar(255), "'unknown'");

		assertEquals("'unknown'", column.getDefaultValueExpression());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCreatingColumnWithNullForColumnName() {
		new Column(null, int8());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCreatingColumnWithEmptyStringForColumnName() {
		new Column("", int8());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCreatingColumnWithNullForColumnType() {
		new Column("id", null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCreatingColumnWithNullHintThrowsException() {
		new Column("id", int8(), null);
	}

	@Test
	public void testGetParentReturnsNullWhenColumnDoesNotBelongToTable() {
		Column column = new Column("id", int8());

		assertEquals(null, column.getParent());
	}

	@Test
	public void testGetParentReturnsParentTableWhenColumnBelongsToTable() {
		Table table = new Table("users");
		Column column = new Column("id", int8());
		table.addColumn(column);

		assertEquals(table, column.getParent());
	}

	@Test
	public void testRenamingColumn() {
		Column column = new Column("id", int8());
		column.rename("uuid");

		assertEquals("uuid", column.getName());
	}

	@Test
	public void testRenamingColumnWhichBelongsToTable() {
		Table table = new Table("users");
		Column column = new Column("id", int8());
		table.addColumn(column);
		column.rename("uuid");

		assertEquals("uuid", column.getName());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testRenamingColumnWithNullForColumnName() {
		new Column("id", int8()).rename(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testRenamingColumnWithEmptyStringForColumnName() {
		new Column("id", int8()).rename("");
	}

	@Test
	public void testThatCopyMethodReturnsCopy() {
		Column column = new Column("id", int8(), IDENTITY, AUTO_INCREMENT, NOT_NULL);
		Column copy = column.copy();

		assertEquals(column, copy);
		assertFalse(column == copy);
	}

	@Test
	public void toStringReturnsSomething() {
		Column column = new Column("id", int8(), "'0'", IDENTITY, NOT_NULL);

		assertFalse(Strings.isNullOrEmpty(column.toString()));
	}

}
