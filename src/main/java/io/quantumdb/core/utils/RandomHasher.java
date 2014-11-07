package io.quantumdb.core.utils;

import java.util.Random;

public class RandomHasher {

	private static final String CONTENTS = "abcdefgh0123456789";

	private final Random random = new Random();

	public String generate() {
		StringBuilder builder = new StringBuilder();
		int position = 0;

		while (builder.length() < 7) {
			position += random.nextInt(CONTENTS.length() * 4) % CONTENTS.length();
			builder.append(CONTENTS.charAt(position % CONTENTS.length()));
		}

		return builder.toString();
	}

}
