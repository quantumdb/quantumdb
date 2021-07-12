package io.quantumdb.cli.xml.operations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import io.quantumdb.cli.xml.ChangelogLoader;
import io.quantumdb.core.schema.operations.CreateView;
import io.quantumdb.core.schema.operations.Operation;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.Version;
import org.junit.Before;
import org.junit.Test;

public class XmlCreateViewTest {

	private Changelog changelog;

	@Before
	public void setUp() {
		this.changelog = new Changelog();

	}

	@Test
	public void testCreateView() throws IOException {
		this.changelog = new ChangelogLoader().load(this.changelog, "src/test/resources/operations/CreateViewChangelog.xml");

		Version version = this.changelog.getRoot().getChild();
		Operation operation = version.getOperation();
		assertTrue(operation instanceof CreateView);
		assertEquals("view1", ((CreateView) operation).getViewName());
		assertEquals("SELECT * FROM table1 WHERE admin = true;", ((CreateView) operation).getQuery());
		assertFalse(((CreateView) operation).isRecursive());
		assertFalse(((CreateView) operation).isTemporary());

		version = version.getChild();
		operation = version.getOperation();
		assertTrue(operation instanceof CreateView);
		assertEquals("view2", ((CreateView) operation).getViewName());
		assertEquals("SELECT * FROM table2 WHERE admin = true;", ((CreateView) operation).getQuery());
		assertTrue(((CreateView) operation).isRecursive());
		assertTrue(((CreateView) operation).isTemporary());

		version = version.getChild();
		assertNull(version);
	}

}
