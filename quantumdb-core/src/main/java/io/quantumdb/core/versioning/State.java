package io.quantumdb.core.versioning;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.state.RefLog;
import lombok.Data;

@Data
public class State {

	private final Catalog catalog;
	private final RefLog refLog;
	private final MigrationFunctions functions;
	private final Changelog changelog;

}
