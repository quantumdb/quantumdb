package io.quantumdb.core.migration.operations;

import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.operations.DropTable;
import io.quantumdb.core.versioning.Version;

public class DropTableMigrator implements SchemaOperationMigrator<DropTable> {

	@Override
	public void expand(Catalog catalog, TableMapping tableMapping, DataMappings dataMappings, Version version, DropTable operation) {
		tableMapping.copyMappingFromParent(version);
		String tableId = tableMapping.getTableId(version, operation.getTableName());
		dataMappings.copy(version)
				.drop(catalog.getTable(tableId));
	}

	@Override
	public void contract(Catalog catalog, TableMapping tableMapping, Version version, DropTable operation) {

	}

}
