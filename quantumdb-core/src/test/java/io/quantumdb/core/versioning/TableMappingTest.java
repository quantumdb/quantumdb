package io.quantumdb.core.versioning;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Sets;
import io.quantumdb.core.utils.RandomHasher;
import org.junit.Before;
import org.junit.Test;

public class TableMappingTest {

	private TableMapping tableMapping;

	@Before
	public void setUp() {
		this.tableMapping = new TableMapping();
	}

	@Test
	public void testTableMappingAfterConstruction() {
		assertTrue(tableMapping.getVersions().isEmpty());
		assertTrue(tableMapping.getTableIds().isEmpty());
	}

	@Test
	public void testAddingTableMapping() {
		Version version = new Version(RandomHasher.generateHash(), null);
		String tableName = "users";
		String alias = RandomHasher.generateHash();

		tableMapping.add(version, tableName, alias);

		assertEquals(alias, tableMapping.getTableId(version, tableName));
		assertEquals(tableName, tableMapping.getTableName(version, alias));
		assertEquals(Sets.newHashSet(version), tableMapping.getVersions());
	}

	@Test
	public void testInheritingTableMappingInSubsequentVersion() {
		String tableName = "users";
		String alias = RandomHasher.generateHash();

		Version version = new Version(RandomHasher.generateHash(), null);
		tableMapping.add(version, tableName, alias);

		Version child = new Version(RandomHasher.generateHash(), version);
		tableMapping.copyMappingFromParent(child);

		assertEquals(alias, tableMapping.getTableId(child, tableName));
		assertEquals(tableName, tableMapping.getTableName(child, alias));
	}

}
