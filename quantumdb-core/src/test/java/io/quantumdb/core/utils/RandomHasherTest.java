package io.quantumdb.core.utils;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.versioning.Version;
import org.junit.Test;

public class RandomHasherTest {

	@Test
	public void testGeneratingRandomHashReturnsNonEmptyString() {
		String hash = RandomHasher.generateHash();
		assertFalse(isNullOrEmpty(hash));
	}

	@Test
	public void testGeneratingRandomHashReturnsDifferentStringsUponMultipleCalls() {
		String hash1 = RandomHasher.generateHash();
		String hash2 = RandomHasher.generateHash();
		assertNotEquals(hash1, hash2);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGeneratingUniqueTableIdWithNullInputThrowsException() {
		RandomHasher.generateTableId(null);
	}

	@Test
	public void testGeneratingUniqueTableIdWithEmptyTableMapping() {
		TableMapping tableMapping = new TableMapping();
		String tableId = RandomHasher.generateTableId(tableMapping);
		assertFalse(isNullOrEmpty(tableId));
	}

	@Test
	public void testGeneratingUniqueTableIdWithFilledTableMapping() {
		TableMapping tableMapping = new TableMapping();
		Version version = new Version(RandomHasher.generateHash(), null);
		tableMapping.set(version, "users", "users");

		String tableId = RandomHasher.generateTableId(tableMapping);
		assertFalse(isNullOrEmpty(tableId));
		assertNotEquals("users", tableId);
	}

}
