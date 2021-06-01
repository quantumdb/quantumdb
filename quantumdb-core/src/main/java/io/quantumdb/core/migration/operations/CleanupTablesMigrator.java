package io.quantumdb.core.migration.operations;

import static com.google.common.base.Preconditions.checkArgument;

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
		checkArgument(refLog.getVersions().size() == 1, "You may only use the cleanup command if there is 1 active version.");
		TransitiveTableMirrorer.mirror(catalog, refLog, version, true, refLog.getTableRefs(version.getParent()).stream()
				.filter(tableRef -> !tableRef.getRefId().equals(tableRef.getName()))
				.map(TableRef::getName).toArray(String[]::new));

	}

}
