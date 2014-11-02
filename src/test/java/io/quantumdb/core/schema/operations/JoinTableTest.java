package io.quantumdb.core.schema.operations;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.junit.Test;

public class JoinTableTest {

	@Test
	public void testJoinTable() {
		JoinTable operation = SchemaOperations.joinTable("users", "u", "id", "name")
				.with("addresses", "a", "u.id = a.user_id", "address")
				.into("users");

		Multimap<String, String> expectedSourceColumns =LinkedHashMultimap.create();
		expectedSourceColumns.putAll("u", Lists.newArrayList("id", "name"));
		expectedSourceColumns.putAll("a", Lists.newArrayList("address"));

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
