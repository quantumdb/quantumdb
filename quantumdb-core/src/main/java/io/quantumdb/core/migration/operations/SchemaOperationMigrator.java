package io.quantumdb.core.migration.operations;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.operations.SchemaOperation;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.Version;

interface SchemaOperationMigrator<T extends SchemaOperation> {

	void migrate(Catalog catalog, RefLog refLog, Version version, T operation);

}
