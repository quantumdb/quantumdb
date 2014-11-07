package io.quantumdb.core.versioning;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import io.quantumdb.core.schema.operations.SchemaOperation;
import io.quantumdb.core.schema.operations.SchemaOperations;
import org.junit.Test;

public class VersionTest {

	@Test
	public void testStaticRootConstructor() {
		Version version = Version.rootVersion();

		assertNotNull(version.getId());
		assertNull(version.getParent());
		assertNull(version.getChild());
		assertNull(version.getChangeSet());
		assertNull(version.getSchemaOperation());
	}

	@Test
	public void testConstructor() {
		Version parent = Version.rootVersion();
		Version version = new Version("some-id", parent);

		assertEquals("some-id", version.getId());
		assertEquals(parent, version.getParent());

		assertNull(version.getChild());
		assertNull(version.getChangeSet());
		assertNull(version.getSchemaOperation());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testConstructorWithNullAsVersionIdWillThrowException() {
		Version parent = Version.rootVersion();
		new Version(null, parent);
	}

	@Test
	public void testConstructorWithNullAsParentIsAllowed() {
		Version version = new Version("some-id", null);

		assertEquals("some-id", version.getId());
		assertNull(version.getParent());
	}

	@Test(expected = IllegalStateException.class)
	public void testThatReLinkingParentThrowsException() {
		Version parent = Version.rootVersion();
		Version version = new Version("some-id", parent);
		version.linkToParent(new Version("some-other-id", null));
	}

	@Test
	public void testApplyMethod() {
		Version parent = Version.rootVersion();

		SchemaOperation operation = SchemaOperations.dropTable("users");
		ChangeSet changeSet = new ChangeSet(operation);
		Version newVersion = parent.apply(operation, changeSet);

		assertNotNull(newVersion.getId());
		assertEquals(parent, newVersion.getParent());
		assertNull(newVersion.getChild());
		assertEquals(newVersion, parent.getChild());
		assertEquals(operation, newVersion.getSchemaOperation());
		assertEquals(changeSet, newVersion.getChangeSet());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testApplyMethodWithNullInputForOperation() {
		Version parent = Version.rootVersion();

		SchemaOperation operation = SchemaOperations.dropTable("users");
		ChangeSet changeSet = new ChangeSet(operation);
		parent.apply(null, changeSet);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testApplyMethodWithNullInputForChangeSet() {
		Version parent = Version.rootVersion();

		SchemaOperation operation = SchemaOperations.dropTable("users");
		parent.apply(operation, null);
	}

}
