package io.quantumdb.core.versioning;

import io.quantumdb.core.schema.definitions.Catalog;
import lombok.Data;

@Data
public class State {

	private final Catalog catalog;
	private final TableMapping tableMapping;
	private final MigrationFunctions functions;
	private final Changelog changelog;

}
