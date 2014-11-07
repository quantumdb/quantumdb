package io.quantumdb.core.versioning;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.common.collect.ImmutableList;
import io.quantumdb.core.schema.operations.SchemaOperation;
import io.quantumdb.core.schema.operations.SchemaOperations;
import org.junit.Test;

public class ChangeSetTest {

	@Test
	public void testSimpleConstructor() {
		SchemaOperation operation1 = SchemaOperations.dropTable("users");
		SchemaOperation operation2 = SchemaOperations.dropTable("addresses");
		ChangeSet changeSet = new ChangeSet(operation1, operation2);

		assertNull(changeSet.getDescription());
		assertEquals(ImmutableList.of(operation1, operation2), changeSet.getSchemaOperations());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSimpleConstructorWithNoArgumentsThrowsException() {
		new ChangeSet();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSimpleConstructorWithNullAsArgumentThrowsException() {
		new ChangeSet(null);
	}

	@Test
	public void testConstructorWithEmptyStringNormalizesItToNull() {
		ChangeSet changeSet = new ChangeSet("", SchemaOperations.dropTable("users"));

		assertNull(changeSet.getDescription());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testConstructorWithNoSchemaOperationsThrowsException() {
		new ChangeSet("");
	}

	@Test
	public void testConstructor() {
		SchemaOperation operation1 = SchemaOperations.dropTable("users");
		SchemaOperation operation2 = SchemaOperations.dropTable("addresses");
		ChangeSet changeSet = new ChangeSet("This is a simple change", operation1, operation2);

		assertEquals("This is a simple change", changeSet.getDescription());
		assertEquals(ImmutableList.of(operation1, operation2), changeSet.getSchemaOperations());
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testThatSchemaOperationsGetterReturnsAnImmutableList() {
		SchemaOperation operation1 = SchemaOperations.dropTable("users");
		ChangeSet changeSet = new ChangeSet("This is a simple change", operation1);

		SchemaOperation operation2 = SchemaOperations.dropTable("addresses");
		changeSet.getSchemaOperations().add(operation2);
	}

}
