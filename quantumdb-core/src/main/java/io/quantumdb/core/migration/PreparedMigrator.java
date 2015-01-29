package io.quantumdb.core.migration;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.migration.operations.SchemaOperationsMigrator;
import io.quantumdb.core.migration.utils.DataMapping;
import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.migration.utils.VersionTraverser;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.versioning.Version;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PreparedMigrator {

	@Data
	public static class Expansion {

		// tableId -> tableName
		private final ImmutableMap<String, String> preTableIdMapping;

		// tableId -> tableName
		private final ImmutableMap<String, String> postTableIdMapping;

		public Set<String> getTableNames() {
			Set<String> altered = Sets.newHashSet();

			// Add tables which were added during expansion.
			Set<String> addedTables = Sets.difference(postTableIdMapping.keySet(), preTableIdMapping.keySet());
			altered.addAll(addedTables);

			preTableIdMapping.keySet().forEach(tableName -> {
				String oldTableId = preTableIdMapping.get(tableName);
				String newTableId = postTableIdMapping.get(tableName);
				if (!oldTableId.equals(newTableId)) {
					altered.add(tableName);
				}
			});

			return altered;
		}
	}

	private final Catalog catalog;
	private final TableMapping tableMapping;
	private final SchemaOperationsMigrator migrator;
	private final Version from;
	private final Version to;
	private final Backend backend;

	PreparedMigrator(Catalog catalog, TableMapping tableMapping, Version from, Version to, Backend backend) {
		this.catalog = catalog;
		this.tableMapping = tableMapping;
		this.from = from;
		this.to = to;
		this.backend = backend;

		this.migrator = new SchemaOperationsMigrator(catalog, tableMapping);
	}

	Expansion expand() throws SQLException {
		List<Version> migrationPath = VersionTraverser.findChildPath(from, to)
				.orElseThrow(() -> new IllegalStateException("No path from " + from.getId() + " to " + to.getId()));

		migrationPath.remove(from);
		migrationPath.stream()
				.filter(version -> version.getParent() != null)
				.forEachOrdered(version -> migrator.expand(version, version.getSchemaOperation()));

		Set<String> preTableIds = tableMapping.getTableIds(from);
		Set<String> postTableIds = tableMapping.getTableIds(to);
		List<Table> newTables = Sets.difference(postTableIds, preTableIds).stream()
				.map(catalog::getTable)
				.collect(Collectors.toList());

		backend.createTables(newTables);

		Map<String, String> preTableIdMapping = preTableIds.stream()
				.collect(Collectors.toMap(tableId -> tableMapping.getTableName(from, tableId), Function.identity()));

		Map<String, String> postTableIdMapping = tableMapping.getTableIds(to).stream()
				.collect(Collectors.toMap(tableId -> tableMapping.getTableName(to, tableId), Function.identity()));

		return new Expansion(ImmutableMap.copyOf(preTableIdMapping), ImmutableMap.copyOf(postTableIdMapping));
	}

	void synchronizeForwards(Expansion expansion) throws SQLException {
		DataMappings dataMappings = migrator.getDataMappings();
		Set<String> alteredTableNames = expansion.getTableNames();

		for (String tableName : alteredTableNames) {
			String tableId = expansion.getPreTableIdMapping().get(tableName);
			Table table = catalog.getTable(tableId);
			Set<DataMapping> mappings = dataMappings.getTransitiveDataMappings(table, DataMappings.Direction.FORWARDS);
			for (DataMapping dataMapping : mappings) {
				backend.installDataMapping(dataMapping);
			}
		}
	}

	void migrateData(Expansion expansion) throws SQLException, InterruptedException {
		DataMappings dataMappings = migrator.getDataMappings();
		Set<String> alteredTableNames = expansion.getTableNames();

		for (String tableName : alteredTableNames) {
			String tableId = expansion.getPreTableIdMapping().get(tableName);
			Table table = catalog.getTable(tableId);
			Set<DataMapping> mappings = dataMappings.getTransitiveDataMappings(table, DataMappings.Direction.FORWARDS);
			for (DataMapping dataMapping : mappings) {
				backend.migrateData(dataMapping);
			}
		}
	}

	void synchronizeBackwards(Expansion expansion) throws SQLException {
		DataMappings dataMappings = migrator.getDataMappings();
		Set<String> alteredTableNames = expansion.getTableNames();

		for (String tableName : alteredTableNames) {
			String tableId = expansion.getPostTableIdMapping().get(tableName);
			Table table = catalog.getTable(tableId);
			Set<DataMapping> mappings = dataMappings.getTransitiveDataMappings(table, DataMappings.Direction.BACKWARDS);
			for (DataMapping dataMapping : mappings) {
				backend.installDataMapping(dataMapping);
			}
		}
	}

}
