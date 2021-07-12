package io.quantumdb.cli.xml.operations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import io.quantumdb.cli.xml.ChangelogLoader;
import io.quantumdb.core.schema.definitions.Column.Hint;
import io.quantumdb.core.schema.definitions.ColumnType.Type;
import io.quantumdb.core.schema.operations.CreateTable;
import io.quantumdb.core.schema.operations.Operation;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.Version;
import org.junit.Before;
import org.junit.Test;

public class XmlCreateTableTest {

	private Changelog changelog;

	@Before
	public void setUp() {
		this.changelog = new Changelog();

	}

	@Test
	public void testCreateTable() throws IOException {
		this.changelog = new ChangelogLoader().load(this.changelog, "src/test/resources/operations/CreateTableChangelog.xml");

		Version version = this.changelog.getRoot().getChild();
		Operation operation = version.getOperation();
		assertTrue(operation instanceof CreateTable);
		assertEquals("table1", ((CreateTable) operation).getTableName());
		assertEquals(5, ((CreateTable) operation).getColumns().size());
		assertEquals(Type.BIGINT, ((CreateTable) operation).getColumns().get(0).getType().getType());
		assertEquals(Type.VARCHAR, ((CreateTable) operation).getColumns().get(1).getType().getType());
		assertEquals(Type.TIMESTAMP, ((CreateTable) operation).getColumns().get(2).getType().getType());
		assertEquals(Type.NUMERIC, ((CreateTable) operation).getColumns().get(3).getType().getType());
		assertEquals(Type.NUMERIC, ((CreateTable) operation).getColumns().get(4).getType().getType());
		assertEquals(1, ((CreateTable) operation).getColumns().get(0).getHints().length);
		assertEquals(0, ((CreateTable) operation).getColumns().get(1).getHints().length);
		assertEquals(1, ((CreateTable) operation).getColumns().get(2).getHints().length);
		assertEquals(1, ((CreateTable) operation).getColumns().get(3).getHints().length);
		assertEquals(3, ((CreateTable) operation).getColumns().get(4).getHints().length);
		assertEquals(Hint.PRIMARY_KEY, ((CreateTable) operation).getColumns().get(0).getHints()[0]);
		assertEquals(Hint.NOT_NULL, ((CreateTable) operation).getColumns().get(2).getHints()[0]);
		assertEquals(Hint.AUTO_INCREMENT, ((CreateTable) operation).getColumns().get(3).getHints()[0]);
		assertEquals(Hint.PRIMARY_KEY, ((CreateTable) operation).getColumns().get(4).getHints()[0]);
		assertEquals(Hint.AUTO_INCREMENT, ((CreateTable) operation).getColumns().get(4).getHints()[1]);
		assertEquals(Hint.NOT_NULL, ((CreateTable) operation).getColumns().get(4).getHints()[2]);
		assertEquals("Test", ((CreateTable) operation).getColumns().get(1).getDefaultValueExpression());

		version = version.getChild();
		assertNull(version);
	}

}
