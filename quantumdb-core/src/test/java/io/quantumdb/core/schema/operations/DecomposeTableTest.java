package io.quantumdb.core.schema.operations;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import org.junit.Test;

public class DecomposeTableTest {

	@Test
	public void testDecomposingIntoTwoTables() {
		DecomposeTable operation = SchemaOperations.decomposeTable("users")
				.into("names", "id", "name")
				.into("addresses", "id", "address");

		ImmutableMultimap<String, String> expectedDecompositions = ImmutableMultimap.<String, String>builder()
				.putAll("names", Lists.newArrayList("id", "name"))
				.putAll("addresses", Lists.newArrayList("id", "address"))
				.build();

		assertEquals("users", operation.getTableName());
		assertEquals(expectedDecompositions, operation.getDecompositions());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDecompositionWithNullForTableNameThrowsException() {
		SchemaOperations.decomposeTable(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDecompositionWithEmptyStringForTableNameThrowsException() {
		SchemaOperations.decomposeTable("");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatDecompositionWithNullForTargetTableNameThrowsException() {
		SchemaOperations.decomposeTable("users")
				.into(null, "id", "name");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatDecompositionWithEmptyStringForTargetTableNameThrowsException() {
		SchemaOperations.decomposeTable("users")
				.into("", "id", "name");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatDecompositionWithoutColumnsThrowsException() {
		SchemaOperations.decomposeTable("users")
				.into("names");
	}

}
