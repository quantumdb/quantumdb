package io.quantumdb.cli.xml.operations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import io.quantumdb.cli.xml.ChangelogLoader;
import io.quantumdb.core.schema.operations.DataOperation;
import io.quantumdb.core.schema.operations.Operation;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.Version;
import org.junit.Before;
import org.junit.Test;

public class XmlQueryTest {

	private Changelog changelog;

	@Before
	public void setUp() {
		this.changelog = new Changelog();

	}

	@Test
	public void testQuery() throws IOException {
		this.changelog = new ChangelogLoader().load(this.changelog, "src/test/resources/operations/QueryChangelog.xml");

		Version version = this.changelog.getRoot().getChild();
		Operation operation = version.getOperation();
		assertTrue(operation instanceof DataOperation);
		assertEquals("INSERT VALUES (\"user1\", 1, \"Nick\") INTO table1;", ((DataOperation) operation).getQuery());

		version = version.getChild();
		assertNull(version);
	}

}
