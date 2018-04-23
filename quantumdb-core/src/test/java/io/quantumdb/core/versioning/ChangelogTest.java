package io.quantumdb.core.versioning;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import io.quantumdb.core.schema.operations.RenameTable;
import io.quantumdb.core.schema.operations.SchemaOperations;
import org.junit.Test;

public class ChangelogTest {

	@Test
	public void testZeroArgumentConstructor() {
		Changelog changelog = new Changelog();

		assertNotNull(changelog.getRoot());
		assertEquals(changelog.getRoot(), changelog.getLastAdded());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatSingleArgumentConstructorWithNullInputThrowsException() {
		new Changelog(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatSingleArgumentConstructorWithEmptyStringThrowsException() {
		new Changelog("");
	}

	@Test
	public void testSingleArgumentConstructorWithVersionId() {
		Changelog changelog = new Changelog("version-1");

		assertNotNull(changelog.getRoot());
		assertEquals(changelog.getRoot(), changelog.getLastAdded());
		assertEquals("version-1", changelog.getRoot().getId());
		assertNull(changelog.getRoot().getChild());
		assertNull(changelog.getRoot().getParent());
	}

	@Test
	public void testAddingChangeSet() {
		Changelog changelog = new Changelog();
		RenameTable renameTable = SchemaOperations.renameTable("users", "customers");
		changelog.addChangeSet("test", "Michael de Jong", renameTable);

		Version version = changelog.getLastAdded();
		Version lookup = changelog.getVersion(version.getId());

		assertEquals(version, lookup);
	}

	@Test
	public void testRetrievingVersion() {
		Changelog changelog = new Changelog();
		Version version = changelog.getLastAdded();
		Version lookup = changelog.getVersion(version.getId());

		assertEquals(version, lookup);
	}

}
