package io.quantumdb.cli.xml.operations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Optional;

import io.quantumdb.cli.xml.ChangelogLoader;
import io.quantumdb.core.schema.definitions.Column.Hint;
import io.quantumdb.core.schema.definitions.ColumnType.Type;
import io.quantumdb.core.schema.operations.AlterColumn;
import io.quantumdb.core.schema.operations.Operation;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.Version;
import org.junit.Before;
import org.junit.Test;

public class XmlAlterColumnTest {

	private Changelog changelog;

	@Before
	public void setUp() {
		this.changelog = new Changelog();

	}

	@Test
	public void testAlterColumn() throws IOException {
		this.changelog = new ChangelogLoader().load(this.changelog, "src/test/resources/operations/AlterColumnChangelog.xml");

		Version version = this.changelog.getRoot().getChild();
		Operation operation = version.getOperation();
		assertTrue(operation instanceof AlterColumn);
		assertEquals(1, ((AlterColumn) operation).getHintsToDrop().size());
		assertEquals(0, ((AlterColumn) operation).getHintsToAdd().size());
		assertEquals(Hint.NOT_NULL, ((AlterColumn) operation).getHintsToDrop().asList().get(0));
		assertEquals(Optional.empty(), ((AlterColumn) operation).getNewColumnName());
		assertEquals(Optional.empty(), ((AlterColumn) operation).getNewColumnType());
		assertEquals(Optional.empty(), ((AlterColumn) operation).getNewDefaultValueExpression());


		version = version.getChild();
		operation = version.getOperation();
		assertTrue(operation instanceof AlterColumn);
		assertEquals(0, ((AlterColumn) operation).getHintsToDrop().size());
		assertEquals(0, ((AlterColumn) operation).getHintsToAdd().size());
		assertEquals("column2", ((AlterColumn) operation).getNewColumnName().get());
		assertEquals(Optional.empty(), ((AlterColumn) operation).getNewColumnType());
		assertEquals(Optional.empty(), ((AlterColumn) operation).getNewDefaultValueExpression());

		version = version.getChild();
		operation = version.getOperation();
		assertTrue(operation instanceof AlterColumn);
		assertEquals(0, ((AlterColumn) operation).getHintsToDrop().size());
		assertEquals(0, ((AlterColumn) operation).getHintsToAdd().size());
		assertEquals(Optional.empty(), ((AlterColumn) operation).getNewColumnName());
		assertEquals(Type.TIMESTAMP, ((AlterColumn) operation).getNewColumnType().get().getType());
		assertEquals(Optional.empty(), ((AlterColumn) operation).getNewDefaultValueExpression());

//		version = version.getChild();
//		operation = version.getOperation();
//		assertTrue(operation instanceof AlterColumn);
//		assertEquals(0, ((AlterColumn) operation).getHintsToDrop().size());
//		assertEquals(1, ((AlterColumn) operation).getHintsToAdd().size());
//		assertEquals(Hint.AUTO_INCREMENT, ((AlterColumn) operation).getHintsToAdd().asList().get(0));
//		assertEquals(Optional.empty(), ((AlterColumn) operation).getNewColumnName());
//		assertEquals(Optional.empty(), ((AlterColumn) operation).getNewColumnType());
//		assertEquals(Optional.empty(), ((AlterColumn) operation).getNewDefaultValueExpression());

//		version = version.getChild();
//		operation = version.getOperation();
//		assertTrue(operation instanceof AlterColumn);
//		assertEquals(0, ((AlterColumn) operation).getHintsToDrop().size());
//		assertEquals(1, ((AlterColumn) operation).getHintsToAdd().size());
//		assertEquals(Hint.PRIMARY_KEY, ((AlterColumn) operation).getHintsToAdd().asList().get(0));
//		assertEquals(Optional.empty(), ((AlterColumn) operation).getNewColumnName());
//		assertEquals(Type.BIGINT, ((AlterColumn) operation).getNewColumnType().get().getType());
//		assertEquals(Optional.empty(), ((AlterColumn) operation).getNewDefaultValueExpression());

		version = version.getChild();
		operation = version.getOperation();
		assertTrue(operation instanceof AlterColumn);
		assertEquals(0, ((AlterColumn) operation).getHintsToDrop().size());
		assertEquals(0, ((AlterColumn) operation).getHintsToAdd().size());
		assertEquals(Optional.empty(), ((AlterColumn) operation).getNewColumnName());
		assertEquals(Optional.empty(), ((AlterColumn) operation).getNewColumnType());
		assertEquals("NOW()", ((AlterColumn) operation).getNewDefaultValueExpression().get());

		version = version.getChild();
		operation = version.getOperation();
		assertTrue(operation instanceof AlterColumn);
		assertEquals(0, ((AlterColumn) operation).getHintsToDrop().size());
		assertEquals(0, ((AlterColumn) operation).getHintsToAdd().size());
		assertEquals(Optional.empty(), ((AlterColumn) operation).getNewColumnName());
		assertEquals(Optional.empty(), ((AlterColumn) operation).getNewColumnType());
		assertEquals("", ((AlterColumn) operation).getNewDefaultValueExpression().get());

		version = version.getChild();
		assertNull(version);
	}

}
