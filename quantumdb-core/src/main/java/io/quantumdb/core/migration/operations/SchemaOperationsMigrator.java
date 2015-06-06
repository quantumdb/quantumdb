package io.quantumdb.core.migration.operations;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.operations.AddColumn;
import io.quantumdb.core.schema.operations.AddForeignKey;
import io.quantumdb.core.schema.operations.AlterColumn;
import io.quantumdb.core.schema.operations.CopyTable;
import io.quantumdb.core.schema.operations.CreateTable;
import io.quantumdb.core.schema.operations.DropColumn;
import io.quantumdb.core.schema.operations.DropForeignKey;
import io.quantumdb.core.schema.operations.DropTable;
import io.quantumdb.core.schema.operations.RenameTable;
import io.quantumdb.core.schema.operations.SchemaOperation;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.versioning.Version;

public class SchemaOperationsMigrator {

	private final Catalog catalog;
	private final TableMapping tableMapping;
	private final DataMappings dataMappings;
	private final Map<Class<? extends SchemaOperation>, SchemaOperationMigrator<?>> migrators;

	public SchemaOperationsMigrator(Catalog catalog, TableMapping tableMapping) {
		this.catalog = catalog;
		this.tableMapping = tableMapping;
		this.dataMappings = new DataMappings(tableMapping, catalog);

		this.migrators = ImmutableMap.<Class<? extends SchemaOperation>, SchemaOperationMigrator<?>>builder()
				.put(AddColumn.class, new AddColumnMigrator())
				.put(AddForeignKey.class, new AddForeignKeyMigrator())
				.put(AlterColumn.class, new AlterColumnMigrator())
				.put(CopyTable.class, new CopyTableMigrator())
				.put(CreateTable.class, new CreateTableMigrator())
				.put(DropTable.class, new DropTableMigrator())
				.put(DropColumn.class, new DropColumnMigrator())
				.put(DropForeignKey.class, new DropForeignKeyMigrator())
//				.put(JoinTable.class, new JoinTableMigrator())
				.put(RenameTable.class, new RenameTableMigrator())
				.build();
	}

	public DataMappings getDataMappings() {
		return dataMappings;
	}

	public <T extends SchemaOperation> void migrate(Version version, T operation) {
		Class<?> type = operation.getClass();
		SchemaOperationMigrator<T> migrator = (SchemaOperationMigrator<T>) migrators.get(type);
		if (migrator == null) {
			throw new UnsupportedOperationException("The operation: " + type + " is not (yet) supported!");
		}
		migrator.migrate(catalog, tableMapping, dataMappings, version, operation);
	}

}
