package io.quantumdb.core.schema.operations;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import com.google.common.collect.Maps;
import org.junit.Test;

public class PartitionTableTest {

	@Test
	public void testPartitionTable() {
		PartitionTable operation = SchemaOperations.partitionTable("users")
				.into("debtors", "credit < 0")
				.into("creditors", "credit > 0");

		Map<String, String> partitions = Maps.newHashMap();
		partitions.put("debtors", "credit < 0");
		partitions.put("creditors", "credit > 0");

		assertEquals("users", operation.getTableName());
		assertEquals(partitions, operation.getPartitions());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatTableNameCannotBeNull() {
		SchemaOperations.partitionTable(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatTableNameCannotBeEmptyString() {
		SchemaOperations.partitionTable("");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatDecompositionTableNameCannotBeNull() {
		SchemaOperations.partitionTable("users")
				.into(null, "credit < 0");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatDecompositionTableNameCannotBeEmptyString() {
		SchemaOperations.partitionTable("users")
				.into("", "credit < 0");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatDecompositionExpressionCannotBeNull() {
		SchemaOperations.partitionTable("users")
				.into("creditors", null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatDecompositionExpressionCannotBeEmptyString() {
		SchemaOperations.partitionTable("users")
				.into("creditors", "");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatYouCannotDecomposeIntoTheSameTableTwice() {
		SchemaOperations.partitionTable("users")
				.into("creditors", "true")
				.into("creditors", "true");
	}

}
