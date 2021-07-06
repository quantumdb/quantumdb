package io.quantumdb.cli.xml.operations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import io.quantumdb.cli.xml.ChangelogLoader;
import io.quantumdb.core.schema.definitions.Column.Hint;
import io.quantumdb.core.schema.definitions.ColumnType.Type;
import io.quantumdb.core.schema.operations.AddColumn;
import io.quantumdb.core.schema.operations.Operation;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.Version;
import org.junit.Before;
import org.junit.Test;

public class XmlAddColumnTest {

	private Changelog changelog;

	@Before
	public void setUp() {
		this.changelog = new Changelog();

	}

	@Test
	public void testAddColumn() throws IOException {
		this.changelog = new ChangelogLoader().load(this.changelog, "src/test/resources/operations/AddColumnChangelog.xml");

		Version version = this.changelog.getRoot().getChild();
		Operation operation = version.getOperation();
		assertTrue(operation instanceof AddColumn);
		assertEquals(0, ((AddColumn) operation).getColumnDefinition().getHints().length);
		assertNull(((AddColumn) operation).getColumnDefinition().getDefaultValueExpression());
		assertEquals(Type.VARCHAR, ((AddColumn) operation).getColumnDefinition().getType().getType());

		version = version.getChild();
		operation = version.getOperation();
		assertTrue(operation instanceof AddColumn);
		assertEquals(0, ((AddColumn) operation).getColumnDefinition().getHints().length);
		assertNull(((AddColumn) operation).getColumnDefinition().getDefaultValueExpression());
		assertEquals(Type.VARCHAR, ((AddColumn) operation).getColumnDefinition().getType().getType());

		version = version.getChild();
		operation = version.getOperation();
		assertTrue(operation instanceof AddColumn);
		assertEquals(1, ((AddColumn) operation).getColumnDefinition().getHints().length);
		assertEquals(Hint.NOT_NULL, ((AddColumn) operation).getColumnDefinition().getHints()[0]);
		assertEquals("test", ((AddColumn) operation).getColumnDefinition().getDefaultValueExpression());
		assertEquals(Type.VARCHAR, ((AddColumn) operation).getColumnDefinition().getType().getType());

		version = version.getChild();
		operation = version.getOperation();
		assertTrue(operation instanceof AddColumn);
		assertEquals(1, ((AddColumn) operation).getColumnDefinition().getHints().length);
		assertEquals(Hint.PRIMARY_KEY, ((AddColumn) operation).getColumnDefinition().getHints()[0]);
		assertNull(((AddColumn) operation).getColumnDefinition().getDefaultValueExpression());
		assertEquals(Type.NUMERIC, ((AddColumn) operation).getColumnDefinition().getType().getType());

		version = version.getChild();
		operation = version.getOperation();
		assertTrue(operation instanceof AddColumn);
		assertEquals(1, ((AddColumn) operation).getColumnDefinition().getHints().length);
		assertEquals(Hint.AUTO_INCREMENT, ((AddColumn) operation).getColumnDefinition().getHints()[0]);
		assertNull(((AddColumn) operation).getColumnDefinition().getDefaultValueExpression());
		assertEquals(Type.NUMERIC, ((AddColumn) operation).getColumnDefinition().getType().getType());

		version = version.getChild();
		operation = version.getOperation();
		assertTrue(operation instanceof AddColumn);
		assertEquals(2, ((AddColumn) operation).getColumnDefinition().getHints().length);
		assertNull(((AddColumn) operation).getColumnDefinition().getDefaultValueExpression());
		assertEquals(Type.NUMERIC, ((AddColumn) operation).getColumnDefinition().getType().getType());

		version = version.getChild();
		operation = version.getOperation();
		assertTrue(operation instanceof AddColumn);
		assertEquals(0, ((AddColumn) operation).getColumnDefinition().getHints().length);
		assertEquals("test", ((AddColumn) operation).getColumnDefinition().getDefaultValueExpression());
		assertEquals(Type.VARCHAR, ((AddColumn) operation).getColumnDefinition().getType().getType());

		version = version.getChild();
		assertNull(version);
	}

}
