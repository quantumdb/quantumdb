package io.quantumdb.cli.xml.operations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import io.quantumdb.cli.xml.ChangelogLoader;
import io.quantumdb.core.schema.operations.DropForeignKey;
import io.quantumdb.core.schema.operations.Operation;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.Version;
import org.junit.Before;
import org.junit.Test;

public class XmlDropForeignKeyTest {

	private Changelog changelog;

	@Before
	public void setUp() {
		this.changelog = new Changelog();

	}

	@Test
	public void testDropForeignKey() throws IOException {
		this.changelog = new ChangelogLoader().load(this.changelog, "src/test/resources/operations/DropForeignKeyChangelog.xml");

		Version version = this.changelog.getRoot().getChild();
		Operation operation = version.getOperation();
		assertTrue(operation instanceof DropForeignKey);
		assertEquals("table1", ((DropForeignKey) operation).getTableName());
		assertEquals("foreign_key1", ((DropForeignKey) operation).getForeignKeyName());

		version = version.getChild();
		assertNull(version);
	}

}
