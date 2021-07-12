package io.quantumdb.cli.xml;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.FileNotFoundException;
import java.io.IOException;

import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.Version;
import org.junit.Before;
import org.junit.Test;

public class ChangelogLoaderTest {

	private Changelog changelog;

	@Before
	public void setUp() {
		this.changelog = new Changelog();
	}

	@Test(expected = FileNotFoundException.class)
	public void testChangeLogNotFound() throws IOException {
		this.changelog = new ChangelogLoader().load(this.changelog, "src/test/resources/NotFoundChangelog.xml");
	}

	@Test
	public void testEmptyChangeLog() throws IOException {
		this.changelog = new ChangelogLoader().load(this.changelog, "src/test/resources/EmptyChangelog.xml");
		assertNull(this.changelog.getRoot().getChangeSet());
		assertNull(this.changelog.getRoot().getChild());
	}

	@Test(expected = IllegalStateException.class)
	public void testDoubleIdChangeLog() throws IOException {
		this.changelog = new ChangelogLoader().load(this.changelog, "src/test/resources/DoubleIdChangelog.xml");
	}

	@Test
	public void testMultipleChangeSetsChangeLog() throws IOException {
		this.changelog = new ChangelogLoader().load(this.changelog, "src/test/resources/MultipleChangesetChangelog.xml");
		Version version = this.changelog.getRoot();
		assertNull(version.getChangeSet());
		version = version.getChild();
		assertNotNull(version.getChangeSet());
		version = version.getChild();
		assertNotNull(version.getChangeSet());
		version = version.getChild();
		assertNotNull(version.getChangeSet());
		version = version.getChild();
		assertNull(version);
	}

}