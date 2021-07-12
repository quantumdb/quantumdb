package io.quantumdb.core.migration.operations;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.operations.CleanupTables;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.TableRef;
import io.quantumdb.core.versioning.Version;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PACKAGE)
public class CleanupTablesMigrator implements SchemaOperationMigrator<CleanupTables> {

	@Override
	public void migrate(Catalog catalog, RefLog refLog, Version version, CleanupTables operation) {
		TransitiveTableMirrorer.mirror(catalog, refLog, version, true, refLog.getTableRefs(version.getParent()).stream()
				.filter(tableRef -> !tableRef.getRefId().equals(tableRef.getName()))
				.map(TableRef::getName).toArray(String[]::new));
	}

}
