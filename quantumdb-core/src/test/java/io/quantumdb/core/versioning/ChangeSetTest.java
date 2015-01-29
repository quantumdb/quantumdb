package io.quantumdb.core.versioning;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class ChangeSetTest {

	@Test
	public void testSingleArgumentConstructor() {
		ChangeSet changeSet = new ChangeSet("Michael de Jong");

		assertEquals("Michael de Jong", changeSet.getAuthor());
		assertNull(changeSet.getDescription());
		assertNotNull(changeSet.getCreated());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatSingleArgumentConstructorWithNullArgumentThrowsException() {
		new ChangeSet(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatSingleArgumentConstructorWithEmptyStringArgumentThrowsException() {
		new ChangeSet("");
	}

	@Test
	public void testThatDoubleArgumentConstructorWithNullArgumentIsAllowed() {
		ChangeSet changeSet = new ChangeSet("Michael de Jong", null);

		assertEquals("Michael de Jong", changeSet.getAuthor());
		assertNull(changeSet.getDescription());
		assertNotNull(changeSet.getCreated());
	}

	@Test
	public void testThatDoubleArgumentConstructorWithEmptyStringArgumentIsAllowed() {
		ChangeSet changeSet = new ChangeSet("Michael de Jong", "");

		assertEquals("Michael de Jong", changeSet.getAuthor());
		assertNull(changeSet.getDescription());
		assertNotNull(changeSet.getCreated());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatTripleArgumentConstructorWithNullArgumentThrowsException() {
		new ChangeSet("Michael de Jong", null, null);
	}

	@Test
	public void testComparison() throws InterruptedException {
		ChangeSet oldest = new ChangeSet("Michael de Jong");

		Thread.sleep(100);
		ChangeSet newest = new ChangeSet("Michael de Jong");

		assertEquals(-1, oldest.compareTo(newest));
	}

}
