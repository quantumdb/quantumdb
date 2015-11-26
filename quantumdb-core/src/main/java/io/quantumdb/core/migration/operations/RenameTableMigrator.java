package io.quantumdb.core.migration.operations;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.operations.RenameTable;
import io.quantumdb.core.state.RefLog;
import io.quantumdb.core.state.RefLog.TableRef;
import io.quantumdb.core.versioning.Version;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PACKAGE)
public class RenameTableMigrator implements SchemaOperationMigrator<RenameTable> {

	@Override
	public void migrate(Catalog catalog, RefLog refLog, Version version, RenameTable operation) {
		refLog.prepareFork(version);
		TableRef tableRef = refLog.getTableRef(version, operation.getTableName());
		tableRef.rename(operation.getNewTableName());
	}

}
