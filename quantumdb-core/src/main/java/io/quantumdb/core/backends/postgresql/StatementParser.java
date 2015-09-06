package io.quantumdb.core.backends.postgresql;

import java.util.List;

import com.google.common.collect.Lists;

class StatementParser {

	private final String statement;
	private String remainder;

	public StatementParser(String statement) {
		this.statement = statement;
		this.remainder = statement;
	}

	public void expect(String keyword) {
		if (!remainder.startsWith(keyword + " ")) {
			throw new IllegalArgumentException("Expected keyword " + keyword + ": " + statement);
		}
		remainder = remainder.substring(keyword.length() + 1, remainder.length());
	}

	public boolean present(String word) {
		boolean present = remainder.startsWith(word + " ");
		if (present) {
			remainder = remainder.substring(word.length() + 1, remainder.length());
		}
		return present;
	}

	public String consume() {
		int pointer = remainder.indexOf(' ');
		String value = remainder.substring(0, pointer);
		remainder = remainder.substring(pointer + 1, remainder.length());
		return value;
	}

	public List<String> consumeGroup(char left, char right, char delimiter) {
		int depth = -1;

		List<String> groups = Lists.newArrayList();
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < remainder.length(); i++) {
			char current = remainder.charAt(i);

			if (current == left) {
				depth++;

				if (depth > 0) {
					builder.append(current);
				}
			}
			else if (current == right) {
				depth--;
				if (depth >= 0) {
					builder.append(current);
				}
				else {
					groups.add(builder.toString());
					return groups;
				}
			}
			else if (depth >= 0) {
				if (current == delimiter) {
					groups.add(builder.toString());
					builder = new StringBuilder();
				}
				else {
					builder.append(current);
				}
			}
		}

		throw new IllegalArgumentException("Cannot consume group in remainder: " + remainder);
	}
}
