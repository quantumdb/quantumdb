package io.quantumdb.cli.xml;

import java.io.IOException;

import io.quantumdb.core.versioning.Changelog;
import org.junit.Before;
import org.junit.Test;

public class MalformedChangelogTest {

	private Changelog changelog;

	@Before
	public void setUp() {
		this.changelog = new Changelog();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMalformedAttribute() throws IOException {
		this.changelog = new ChangelogLoader().load(this.changelog, "src/test/resources/malformed-syntax/MalformedAttributeChangelog.xml");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMalformedChangelog() throws IOException {
		this.changelog = new ChangelogLoader().load(this.changelog, "src/test/resources/malformed-syntax/MalformedChangelog.xml");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMalformedChangeSet() throws IOException {
		this.changelog = new ChangelogLoader().load(this.changelog, "src/test/resources/malformed-syntax/MalformedChangeSetChangelog.xml");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMalformedChild() throws IOException {
		this.changelog = new ChangelogLoader().load(this.changelog, "src/test/resources/malformed-syntax/MalformedChildChangelog.xml");
	}

}