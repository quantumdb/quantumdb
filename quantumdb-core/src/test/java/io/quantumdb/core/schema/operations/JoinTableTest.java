package io.quantumdb.core.schema.operations;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Test;

public class JoinTableTest {

	@Test
	public void testJoinTable() {
		JoinTable operation = SchemaOperations.joinTable("users", "u", "id", "name")
				.with("addresses", "a", "u.id = a.user_id", "address")
				.into("users");

		ImmutableMap<String, List<String>> expectedSourceColumns = ImmutableMap.<String, List<String>>builder()
				.put("u", Lists.newArrayList("id", "name"))
				.put("a", Lists.newArrayList("address"))
				.build();

		Map<String, String> expectedJoinConditions = ImmutableMap.of("a", "u.id = a.user_id");
		Map<String, String> expectedSourceTables = ImmutableMap.of("u", "users", "a", "addresses");

		assertEquals("users", operation.getTargetTableName());
		assertEquals(expectedJoinConditions, operation.getJoinConditions());
		assertEquals(expectedSourceTables, operation.getSourceTables());
		assertEquals(expectedSourceColumns, operation.getSourceColumns());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAliasesMustBeUnique() {
		SchemaOperations.joinTable("users", "a", "id", "name")
				.with("addresses", "a", "a.id = a.user_id", "address");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatYouMustSpecifyAtLeastOneColumn() {
		SchemaOperations.joinTable("users", "a")
				.with("addresses", "a", "a.id = a.user_id")
				.into("users");
	}

}
