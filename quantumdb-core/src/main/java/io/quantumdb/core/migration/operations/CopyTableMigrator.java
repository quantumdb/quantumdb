package io.quantumdb.core.migration.operations;

import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.CopyTable;
import io.quantumdb.core.utils.RandomHasher;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.versioning.Version;

class CopyTableMigrator implements SchemaOperationMigrator<CopyTable> {

	@Override
	public void expand(Catalog catalog, TableMapping tableMapping, DataMappings dataMappings, Version version, CopyTable operation) {
		String tableId = RandomHasher.generateTableId(tableMapping);
		String sourceTableName = operation.getSourceTableName();
		String targetTableName = operation.getTargetTableName();

		tableMapping.copyMappingFromParent(version);
		String sourceTableId = tableMapping.getTableId(version, sourceTableName);

		Table table = catalog.getTable(sourceTableId);
		Table copy = table.copy()
				.rename(tableId);

		dataMappings.copy(version);

		for (ForeignKey foreignKey : table.getForeignKeys()) {
			String referredTableId = foreignKey.getReferredTableName();
			String referredTableName = tableMapping.getTableName(version, referredTableId);
			referredTableId = tableMapping.getTableId(version, referredTableName);

			Table referredTable = catalog.getTable(referredTableId);
			copy.addForeignKey(foreignKey.getReferencingColumns().toArray(new String[0]))
					.referencing(referredTable, foreignKey.getReferredColumns().toArray(new String[0]));
		}

		catalog.addTable(copy);
		tableMapping.copy(version, sourceTableName, targetTableName, tableId);

		// TODO: Add pipeline mappings...
	}

	@Override
	public void contract(Catalog catalog, TableMapping tableMapping, Version version, CopyTable operation) {
		throw new UnsupportedOperationException();
	}

}
