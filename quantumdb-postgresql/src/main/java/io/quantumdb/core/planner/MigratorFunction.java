package io.quantumdb.core.planner;

import java.util.LinkedHashMap;

import lombok.Data;

@Data
public class MigratorFunction {

	public enum Stage {
		INITIAL, CONSECUTIVE
	}

	private final String name;
	private final LinkedHashMap<String, String> parameters;
	private final String createStatement;
	private final String dropStatement;

}
