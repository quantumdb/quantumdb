package io.quantumdb.core.migration.operations;

import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.operations.RenameTable;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.versioning.Version;

/**
 * TODO: We don't actually rename tables. We just manipulate the table mapping... Should we "fix" this?
 */
public class RenameTableMigrator implements SchemaOperationMigrator<RenameTable> {

	@Override
	public void expand(Catalog catalog, TableMapping tableMapping, DataMappings dataMappings, Version version, RenameTable operation) {
		String sourceTableName = operation.getTableName();
		String targetTableName = operation.getNewTableName();

		tableMapping.copyMappingFromParent(version);
		tableMapping.rename(version, sourceTableName, targetTableName);
	}

	@Override
	public void contract(Catalog catalog, TableMapping tableMapping, Version version, RenameTable operation) {
		// Do nothing...
	}

}
