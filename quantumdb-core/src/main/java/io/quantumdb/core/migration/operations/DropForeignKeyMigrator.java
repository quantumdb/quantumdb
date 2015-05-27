package io.quantumdb.core.migration.operations;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.DropForeignKey;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.versioning.Version;

class DropForeignKeyMigrator implements SchemaOperationMigrator<DropForeignKey> {

	@Override
	public void expand(Catalog catalog, TableMapping tableMapping, DataMappings dataMappings, Version version, DropForeignKey operation) {
		String tableName = operation.getTableName();
		TransitiveTableMirrorer.mirror(catalog, tableMapping, version, tableName);
		dataMappings.copy(version);

		String tableId = tableMapping.getTableId(version, tableName);
		Table table = catalog.getTable(tableId);

		ForeignKey matchedForeignKey = table.getForeignKeys().stream()
				.filter(foreignKey -> {
					ImmutableList<String> referencingColumns = foreignKey.getReferencingColumns();
					return referencingColumns.containsAll(Sets.newHashSet(operation.getReferringColumnNames()));
				})
				.findFirst()
				.orElseThrow(() -> new IllegalStateException("No foreign key exists in table: " + tableName
						+ " for the column(s): " + Joiner.on(", ").join(operation.getReferringColumnNames())));

		matchedForeignKey.drop();
	}

	@Override
	public void contract(Catalog catalog, TableMapping tableMapping, Version version, DropForeignKey operation) {
		throw new UnsupportedOperationException();
	}

}
