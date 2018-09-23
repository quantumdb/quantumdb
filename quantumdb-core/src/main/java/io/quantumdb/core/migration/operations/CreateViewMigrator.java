package io.quantumdb.core.migration.operations;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.View;
import io.quantumdb.core.schema.operations.CreateView;
import io.quantumdb.core.utils.RandomHasher;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.Version;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PACKAGE)
class CreateViewMigrator implements SchemaOperationMigrator<CreateView> {

	@Override
	public void migrate(Catalog catalog, RefLog refLog, Version version, CreateView operation) {
		String refId = RandomHasher.generateRefId(refLog);
		String viewName = operation.getViewName();

		refLog.fork(version);
		refLog.addView(viewName, refId, version);

		View view = new View(refId, operation.getQuery());
		catalog.addView(view);
	}

}
