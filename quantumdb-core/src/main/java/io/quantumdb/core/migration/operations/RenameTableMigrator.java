package io.quantumdb.core.migration.operations;

import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.operations.RenameTable;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.versioning.Version;

public class RenameTableMigrator implements SchemaOperationMigrator<RenameTable> {

	@Override
	public void migrate(Catalog catalog, TableMapping tableMapping, DataMappings dataMappings, Version version,
			RenameTable operation) {
		String sourceTableName = operation.getTableName();
		String targetTableName = operation.getNewTableName();

		tableMapping.copyMappingFromParent(version);
		tableMapping.rename(version, sourceTableName, targetTableName);
	}

}
