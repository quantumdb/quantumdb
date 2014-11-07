package io.quantumdb.core.versioning;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.GenericColumnTypes.int8;
import static io.quantumdb.core.schema.definitions.GenericColumnTypes.varchar;
import static org.junit.Assert.assertEquals;

import io.quantumdb.core.schema.operations.CreateTable;
import io.quantumdb.core.schema.operations.SchemaOperations;
import org.junit.Test;

public class ChangelogTest {

	@Test
	public void testDefiningChangeSet() {
		Changelog changelog = new Changelog();
		Version parent = changelog.getCurrent();

		CreateTable operation = SchemaOperations.createTable("users")
				.with("id", int8(), IDENTITY, AUTO_INCREMENT, NOT_NULL)
				.with("name", varchar(255), NOT_NULL);

		ChangeSet changeSet = changelog.addChangeSet("ChangeSet with a description", operation);

		Version version = changelog.getCurrent();

		assertEquals(changeSet, version.getChangeSet());
		assertEquals(parent, version.getParent());
		assertEquals(operation, version.getSchemaOperation());
	}

	@Test
	public void testDefiningChangeSetWithMultipleOperations() {
		Changelog changelog = new Changelog();
		Version parent = changelog.getCurrent();

		CreateTable operation1 = SchemaOperations.createTable("users")
				.with("id", int8(), IDENTITY, AUTO_INCREMENT, NOT_NULL)
				.with("name", varchar(255), NOT_NULL);

		CreateTable operation2 = SchemaOperations.createTable("addresses")
				.with("id", int8(), IDENTITY, AUTO_INCREMENT, NOT_NULL)
				.with("address", varchar(255), NOT_NULL);

		ChangeSet changeSet = changelog.addChangeSet(operation1, operation2);

		Version version = changelog.getCurrent();
		assertEquals(changeSet, version.getChangeSet());
		assertEquals(operation2, version.getSchemaOperation());

		Version previous = version.getParent();
		assertEquals(changeSet, previous.getChangeSet());
		assertEquals(operation1, previous.getSchemaOperation());
		assertEquals(parent, previous.getParent());
	}

}
