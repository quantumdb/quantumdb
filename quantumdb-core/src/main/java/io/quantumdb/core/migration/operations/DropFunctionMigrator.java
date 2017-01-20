package io.quantumdb.core.migration.operations;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.operations.DropFunction;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.Version;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PACKAGE)
class DropFunctionMigrator implements SchemaOperationMigrator<DropFunction> {

	@Override
	public void migrate(Catalog catalog, RefLog refLog, Version version, DropFunction operation) {
		catalog.removeFunction(operation.getFunctionName(), operation.getParameterTypes());
	}

}
