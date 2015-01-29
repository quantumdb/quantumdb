package io.quantumdb.core.utils;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Random;
import java.util.Set;

import io.quantumdb.core.versioning.TableMapping;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RandomHasher {

	private static final String CONTENTS = "0123456789abcdef";
	private static final int LENGTH = 10;

	public static String generateHash() {
		Random random = new Random();
		StringBuilder builder = new StringBuilder();
		int position = 0;

		while (builder.length() < LENGTH) {
			position += random.nextInt(CONTENTS.length() * 4) % CONTENTS.length();
			builder.append(CONTENTS.charAt(position % CONTENTS.length()));
		}

		return builder.toString();
	}

	public static String generateTableId(TableMapping tableMapping) {
		checkArgument(tableMapping != null, "You must specify a 'tableMapping'.");

		Set<String> tableIds = tableMapping.getTableIds();
		String hash = "table_" + generateHash();
		while (tableIds.contains(hash)) {
			hash = "table_" + generateHash();
		}
		return hash;
	}

}
