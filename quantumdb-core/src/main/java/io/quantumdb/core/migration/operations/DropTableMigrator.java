package io.quantumdb.core.migration.operations;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.operations.DropTable;
import io.quantumdb.core.state.RefLog;
import io.quantumdb.core.versioning.Version;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PACKAGE)
public class DropTableMigrator implements SchemaOperationMigrator<DropTable> {

	@Override
	public void migrate(Catalog catalog, RefLog refLog, Version version, DropTable operation) {
		refLog.prepareFork(version);
		refLog.dropTable(version, operation.getTableName());
	}

}
