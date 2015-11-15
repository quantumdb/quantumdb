package io.quantumdb.query.rewriter;

import java.sql.SQLException;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

@Experimental
public class PostgresqlQueryRewriter implements QueryRewriter {

	private static class State {

		private StringBuilder output;
		private String query;
		private int lastProcessedCursor;
		private int cursor;
		private String previousWord;
		private Deque<Character> stack;

		private State(String query) {
			this.query = query;
			this.output = new StringBuilder();
			this.cursor = 0;
			this.lastProcessedCursor = 0;
			this.previousWord = "";
			this.stack = new LinkedList<>();
		}

	}

	private static final String DEFAULT_SCHEMA = "public";

	private final Map<String, String> tableMapping;

	public PostgresqlQueryRewriter() {
		this.tableMapping = new HashMap<>();
	}

	public void setTableMapping(Map<String, String> newTableMapping) {
		tableMapping.clear();
		tableMapping.putAll(newTableMapping);
	}

	public String rewrite(String query) throws SQLException {
		if (query == null) {
			throw new IllegalArgumentException("You must specify specify a 'query'.");
		}

		State state = new State(query);
		for (state.cursor = 0; state.cursor < query.length(); state.cursor++) {
			char currentCharacter = query.charAt(state.cursor);

				switch (currentCharacter) {
					case '\'':
					case '\"':
					case '`':
						Character onStack = state.stack.peekFirst();
						if (onStack != null && onStack.equals(currentCharacter)) {
							state.stack.removeFirst();
						}
						else {
							state.stack.addFirst(currentCharacter);
						}
						processWordStartOrMiddle(state);
						break;
					case ' ':
					case ',':
					case ';':
						if (state.stack.isEmpty()) {
							processWordEnding(state);
						}
						processWordStartOrMiddle(state);
						break;
					default:
						processWordStartOrMiddle(state);
						break;
				}
		}

		processWordEnding(state);

		return state.output.toString();
	}

	private void processWordEnding(State state) throws SQLException {
		String word = state.query.substring(state.lastProcessedCursor, state.cursor);

		if (word.trim().isEmpty()) {
			state.output.append(word);
		}
		else {
			state.output.append(processWord(state, word));
		}

		state.lastProcessedCursor = state.cursor;
	}

	private void processWordStartOrMiddle(State state) {
		String word = state.query.substring(state.lastProcessedCursor, state.cursor);

		if (word.trim().isEmpty()) {
			state.output.append(word);
			state.lastProcessedCursor = state.cursor;
		}
	}

	private String processWord(State state, String word) throws SQLException {
		if (state.previousWord.equalsIgnoreCase("from") || state.previousWord.equalsIgnoreCase("join") ||
				state.previousWord.equalsIgnoreCase("update") || state.previousWord.equalsIgnoreCase("into")) {

			boolean isQuoted = isQuoted(word);
			if (isQuoted) {
				word = word.substring(1, word.length() - 1);
			}

			String schema = null;
			if (word != null && word.startsWith(DEFAULT_SCHEMA + ".")) {
				schema = DEFAULT_SCHEMA;
				word = word.substring(DEFAULT_SCHEMA.length() + 1, word.length());
			}

			String tableName = tableMapping.get(word);

			if (!isQuoted && tableName == null) {
				for (Map.Entry<String, String> entry : tableMapping.entrySet()) {
					if (entry.getKey().equalsIgnoreCase(word)) {
						tableName = entry.getValue();
						break;
					}
				}
			}

			if (tableName == null) {
				//throw new SQLException("No table with name: '" + word + "' exists.");
			}
			else {
				word = tableName;
			}

			if (schema != null) {
				word = schema + "." + word;
			}

			if (isQuoted) {
				word = "'" + word + "'";
			}
		}

		state.previousWord = word;
		return word;
	}

	private boolean isQuoted(String word) {
		char first = word.charAt(0);
		char last = word.charAt(word.length() - 1);

		if (first != last) {
			return false;
		}

		return first == '\'';
	}

}
