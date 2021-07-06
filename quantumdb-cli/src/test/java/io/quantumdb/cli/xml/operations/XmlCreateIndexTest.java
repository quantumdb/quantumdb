package io.quantumdb.cli.xml.operations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import io.quantumdb.cli.xml.ChangelogLoader;
import io.quantumdb.core.schema.operations.CreateIndex;
import io.quantumdb.core.schema.operations.Operation;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.Version;
import org.junit.Before;
import org.junit.Test;

public class XmlCreateIndexTest {

	private Changelog changelog;

	@Before
	public void setUp() {
		this.changelog = new Changelog();

	}

	@Test
	public void testCreateIndex() throws IOException {
		this.changelog = new ChangelogLoader().load(this.changelog, "src/test/resources/operations/CreateIndexChangelog.xml");

		Version version = this.changelog.getRoot().getChild();
		Operation operation = version.getOperation();
		assertTrue(operation instanceof CreateIndex);
		assertEquals("table1", ((CreateIndex) operation).getTableName());
		assertEquals(1, ((CreateIndex) operation).getColumns().size());
		assertEquals("column1", ((CreateIndex) operation).getColumns().asList().get(0));
		assertFalse(((CreateIndex) operation).isUnique());

		version = version.getChild();
		operation = version.getOperation();
		assertTrue(operation instanceof CreateIndex);
		assertEquals("table1", ((CreateIndex) operation).getTableName());
		assertEquals(2, ((CreateIndex) operation).getColumns().size());
		assertEquals("column1", ((CreateIndex) operation).getColumns().asList().get(0));
		assertEquals("column2", ((CreateIndex) operation).getColumns().asList().get(1));
		assertTrue(((CreateIndex) operation).isUnique());

		version = version.getChild();
		assertNull(version);
	}

}
