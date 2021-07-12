package io.quantumdb.cli.xml.operations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import io.quantumdb.cli.xml.ChangelogLoader;
import io.quantumdb.core.schema.definitions.ForeignKey.Action;
import io.quantumdb.core.schema.operations.AddForeignKey;
import io.quantumdb.core.schema.operations.Operation;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.Version;
import org.junit.Before;
import org.junit.Test;

public class XmlAddForeignKeyTest {

	private Changelog changelog;

	@Before
	public void setUp() {
		this.changelog = new Changelog();

	}

	@Test
	public void testAddForeignKey() throws IOException {
		this.changelog = new ChangelogLoader().load(this.changelog, "src/test/resources/operations/AddForeignKeyChangelog.xml");

		Version version = this.changelog.getRoot().getChild();
		Operation operation = version.getOperation();
		assertTrue(operation instanceof AddForeignKey);
		assertEquals("table1", ((AddForeignKey) operation).getReferringTableName());
		assertEquals("table2", ((AddForeignKey) operation).getReferencedTableName());
		assertEquals(1, ((AddForeignKey) operation).getReferringColumnNames().length);
		assertEquals(1, ((AddForeignKey) operation).getReferencedColumnNames().length);
		assertEquals("column1", ((AddForeignKey) operation).getReferringColumnNames()[0]);
		assertEquals("column1", ((AddForeignKey) operation).getReferencedColumnNames()[0]);
		assertEquals("foreign_key1", ((AddForeignKey) operation).getName());
		assertEquals(Action.NO_ACTION, ((AddForeignKey) operation).getOnDelete());
		assertEquals(Action.NO_ACTION, ((AddForeignKey) operation).getOnUpdate());

		version = version.getChild();
		operation = version.getOperation();
		assertTrue(operation instanceof AddForeignKey);
		assertEquals("table1", ((AddForeignKey) operation).getReferringTableName());
		assertEquals("table2", ((AddForeignKey) operation).getReferencedTableName());
		assertEquals(2, ((AddForeignKey) operation).getReferringColumnNames().length);
		assertEquals(2, ((AddForeignKey) operation).getReferencedColumnNames().length);
		assertEquals("column1", ((AddForeignKey) operation).getReferringColumnNames()[0]);
		assertEquals("column1", ((AddForeignKey) operation).getReferencedColumnNames()[0]);
		assertEquals("column2", ((AddForeignKey) operation).getReferringColumnNames()[1]);
		assertEquals("column2", ((AddForeignKey) operation).getReferencedColumnNames()[1]);
		assertEquals("foreign_key2", ((AddForeignKey) operation).getName());
		assertEquals(Action.NO_ACTION, ((AddForeignKey) operation).getOnDelete());
		assertEquals(Action.NO_ACTION, ((AddForeignKey) operation).getOnUpdate());

		version = version.getChild();
		operation = version.getOperation();
		assertTrue(operation instanceof AddForeignKey);
		assertEquals("table1", ((AddForeignKey) operation).getReferringTableName());
		assertEquals("table2", ((AddForeignKey) operation).getReferencedTableName());
		assertEquals(1, ((AddForeignKey) operation).getReferringColumnNames().length);
		assertEquals(1, ((AddForeignKey) operation).getReferencedColumnNames().length);
		assertEquals("column1", ((AddForeignKey) operation).getReferringColumnNames()[0]);
		assertEquals("column1", ((AddForeignKey) operation).getReferencedColumnNames()[0]);
		assertEquals("foreign_key3", ((AddForeignKey) operation).getName());
		assertEquals(Action.RESTRICT, ((AddForeignKey) operation).getOnDelete());
		assertEquals(Action.CASCADE, ((AddForeignKey) operation).getOnUpdate());

		version = version.getChild();
		operation = version.getOperation();
		assertTrue(operation instanceof AddForeignKey);
		assertEquals("table1", ((AddForeignKey) operation).getReferringTableName());
		assertEquals("table2", ((AddForeignKey) operation).getReferencedTableName());
		assertEquals(1, ((AddForeignKey) operation).getReferringColumnNames().length);
		assertEquals(1, ((AddForeignKey) operation).getReferencedColumnNames().length);
		assertEquals("column1", ((AddForeignKey) operation).getReferringColumnNames()[0]);
		assertEquals("column1", ((AddForeignKey) operation).getReferencedColumnNames()[0]);
		assertEquals("foreign_key4", ((AddForeignKey) operation).getName());
		assertEquals(Action.SET_DEFAULT, ((AddForeignKey) operation).getOnDelete());
		assertEquals(Action.SET_NULL, ((AddForeignKey) operation).getOnUpdate());

		version = version.getChild();
		operation = version.getOperation();
		assertTrue(operation instanceof AddForeignKey);
		assertEquals("table1", ((AddForeignKey) operation).getReferringTableName());
		assertEquals("table2", ((AddForeignKey) operation).getReferencedTableName());
		assertEquals(1, ((AddForeignKey) operation).getReferringColumnNames().length);
		assertEquals(1, ((AddForeignKey) operation).getReferencedColumnNames().length);
		assertEquals("column1", ((AddForeignKey) operation).getReferringColumnNames()[0]);
		assertEquals("column1", ((AddForeignKey) operation).getReferencedColumnNames()[0]);
		assertEquals("foreign_key5", ((AddForeignKey) operation).getName());
		assertEquals(Action.NO_ACTION, ((AddForeignKey) operation).getOnDelete());
		assertEquals(Action.NO_ACTION, ((AddForeignKey) operation).getOnUpdate());

		version = version.getChild();
		assertNull(version);
	}

}
