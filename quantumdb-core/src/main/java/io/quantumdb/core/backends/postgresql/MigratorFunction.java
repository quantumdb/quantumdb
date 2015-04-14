package io.quantumdb.core.backends.postgresql;

import java.util.List;

import lombok.Data;

@Data
public class MigratorFunction {

	public static enum Stage {
		INITIAL, CONSECUTIVE
	}

	private final String name;
	private final List<String> parameters;
	private final String createStatement;
	private final String dropStatement;

}
