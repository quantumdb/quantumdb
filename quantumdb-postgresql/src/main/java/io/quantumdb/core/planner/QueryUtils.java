package io.quantumdb.core.planner;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class QueryUtils {

	public static String quoted(String input) {
		if (input == null) {
			return null;
		}
		else if (input.length() >= 2 && input.charAt(0) == '\"' && input.charAt(input.length() - 1) == '\"') {
			return input;
		}
		return "\"" + input + "\"";
	}

}
