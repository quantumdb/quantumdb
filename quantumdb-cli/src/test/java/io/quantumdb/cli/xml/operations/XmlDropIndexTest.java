package io.quantumdb.cli.xml.operations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import io.quantumdb.cli.xml.ChangelogLoader;
import io.quantumdb.core.schema.operations.DropIndex;
import io.quantumdb.core.schema.operations.Operation;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.Version;
import org.junit.Before;
import org.junit.Test;

public class XmlDropIndexTest {

	private Changelog changelog;

	@Before
	public void setUp() {
		this.changelog = new Changelog();

	}

	@Test
	public void testDropIndex() throws IOException {
		this.changelog = new ChangelogLoader().load(this.changelog, "src/test/resources/operations/DropIndexChangelog.xml");

		Version version = this.changelog.getRoot().getChild();
		Operation operation = version.getOperation();
		assertTrue(operation instanceof DropIndex);
		assertEquals("table1", ((DropIndex) operation).getTableName());
		assertEquals(1, ((DropIndex) operation).getColumns().length);
		assertEquals("column1", ((DropIndex) operation).getColumns()[0]);

		version = version.getChild();
		operation = version.getOperation();
		assertTrue(operation instanceof DropIndex);
		assertEquals("table1", ((DropIndex) operation).getTableName());
		assertEquals(2, ((DropIndex) operation).getColumns().length);
		assertEquals("column1", ((DropIndex) operation).getColumns()[0]);
		assertEquals("column2", ((DropIndex) operation).getColumns()[1]);

		version = version.getChild();
		assertNull(version);
	}

}
