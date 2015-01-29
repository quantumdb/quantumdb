package io.quantumdb.core.migration.utils;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.versioning.TableMapping;
import org.junit.Before;
import org.junit.Test;

public class DataMappingsTest {

	private Catalog catalog;
	private TableMapping tableMapping;
	private DataMappings dataMappings;

	private Table table1;
	private Table table2;
	private Table table3;
	private Table table4;

	@Before
	public void setUp() {
		this.catalog = new Catalog("test-db");
		this.tableMapping = new TableMapping();
		this.dataMappings = new DataMappings(tableMapping, catalog);

		this.table1 = new Table("users_v1");
		this.table2 = new Table("users_v2");
		this.table3 = new Table("users_v3");
		this.table4 = new Table("users_v4");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddingTransformationWithNullForSourceTable() {
		dataMappings.add(null, "name", table2, "full_name", DataMapping.Transformation.createNop());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddingTransformationWithNullForSourceColumnName() {
		dataMappings.add(table1, null, table2, "full_name", DataMapping.Transformation.createNop());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddingTransformationWithEmptyStringForSourceColumnName() {
		dataMappings.add(table1, "", table2, "full_name", DataMapping.Transformation.createNop());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddingTransformationWithNullForTargetTableId() {
		dataMappings.add(table1, "name", null, "full_name", DataMapping.Transformation.createNop());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddingTransformationWithNullForTargetColumnName() {
		dataMappings.add(table1, "name", table2, null, DataMapping.Transformation.createNop());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddingTransformationWithEmptyStringForTargetColumnName() {
		dataMappings.add(table1, "name", table2, "", DataMapping.Transformation.createNop());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddingTransformationWithNullForTransformation() {
		dataMappings.add(table1, "name", table2, "full_name", null);
	}

	@Test
	public void testAddingTransformation() {
		dataMappings.add(table1, "name", table2, "full_name", DataMapping.Transformation.createNop());

		Map<Table, DataMapping> actual = dataMappings.getTransitiveDataMappings(table1, DataMappings.Direction.FORWARDS)
				.stream()
				.collect(Collectors.toMap(value -> value.getTargetTable(), Function.<DataMapping>identity()));

		DataMapping expected = new DataMapping(table1, table2)
				.setColumnMapping("name", "full_name", DataMapping.Transformation.createNop());

		assertEquals(1, actual.size());
		assertEquals(expected, actual.get(table2));
	}

	@Test
	public void testThatTransitiveDataMappingCanBeRetrieved() {
		dataMappings.add(table1, "name", table2, "full_name", DataMapping.Transformation.createNop());
		dataMappings.add(table2, "full_name", table3, "full_user_name", DataMapping.Transformation.createNop());

		Map<Table, DataMapping> actual = dataMappings.getTransitiveDataMappings(table1, DataMappings.Direction.FORWARDS)
				.stream()
				.collect(Collectors.toMap(value -> value.getTargetTable(), Function.<DataMapping>identity()));

		DataMapping expected = new DataMapping(table1, table3)
				.setColumnMapping("name", "full_user_name", DataMapping.Transformation.createNop());

		assertEquals(1, actual.size());
		assertEquals(expected, actual.get(table3));
	}

	@Test
	public void testThatDiamondPathDataMappingCanBeRetrieved() {
		dataMappings.add(table1, "name", table2, "name", DataMapping.Transformation.createNop());
		dataMappings.add(table1, "date_of_birth", table3, "date_of_birth", DataMapping.Transformation.createNop());
		dataMappings.add(table2, "name", table4, "name", DataMapping.Transformation.createNop());
		dataMappings.add(table3, "date_of_birth", table4, "date_of_birth", DataMapping.Transformation.createNop());

		Map<Table, DataMapping> actual = dataMappings.getTransitiveDataMappings(table1, DataMappings.Direction.FORWARDS)
				.stream()
				.collect(Collectors.toMap(value -> value.getTargetTable(), Function.<DataMapping>identity()));

		DataMapping expected = new DataMapping(table1, table4)
				.setColumnMapping("name", "name", DataMapping.Transformation.createNop())
				.setColumnMapping("date_of_birth", "date_of_birth", DataMapping.Transformation.createNop());

		assertEquals(1, actual.size());
		assertEquals(expected, actual.get(table4));
	}

}
