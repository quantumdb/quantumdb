package io.quantumdb.core.migration.operations;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.operations.DropView;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.Version;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PACKAGE)
public class DropViewMigrator implements SchemaOperationMigrator<DropView> {

	@Override
	public void migrate(Catalog catalog, RefLog refLog, Version version, DropView operation) {
		refLog.fork(version);
		refLog.dropView(version, operation.getViewName());
	}

}
