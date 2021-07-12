package io.quantumdb.cli.xml.operations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import io.quantumdb.cli.xml.ChangelogLoader;
import io.quantumdb.core.schema.operations.Operation;
import io.quantumdb.core.schema.operations.RenameTable;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.Version;
import org.junit.Before;
import org.junit.Test;

public class XmlRenameTableTest {

	private Changelog changelog;

	@Before
	public void setUp() {
		this.changelog = new Changelog();

	}

	@Test
	public void testRenameTable() throws IOException {
		this.changelog = new ChangelogLoader().load(this.changelog, "src/test/resources/operations/RenameTableChangelog.xml");

		Version version = this.changelog.getRoot().getChild();
		Operation operation = version.getOperation();
		assertTrue(operation instanceof RenameTable);
		assertEquals("table1", ((RenameTable) operation).getTableName());
		assertEquals("table2", ((RenameTable) operation).getNewTableName());

		version = version.getChild();
		assertNull(version);
	}

}
