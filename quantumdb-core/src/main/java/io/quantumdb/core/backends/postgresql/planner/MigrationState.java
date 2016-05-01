package io.quantumdb.core.backends.postgresql.planner;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;

class MigrationState {

	static enum Progress {
		PENDING,
		PARTIALLY,
		DONE
	}

	private final Catalog catalog;
	private final Multimap<String, String> migratedColumns;

	public MigrationState(Catalog catalog) {
		this.catalog = catalog;
		this.migratedColumns = HashMultimap.create();
	}

	public Progress getProgress(String tableName) {
		Collection<String> migrated = migratedColumns.get(tableName);
		if (migrated.isEmpty()) {
			return Progress.PENDING;
		}

		Table table = catalog.getTable(tableName);
		Set<String> columns = table.getColumns().stream()
				.map(Column::getName)
				.collect(Collectors.toSet());

		if (migrated.containsAll(columns)) {
			return Progress.DONE;
		}
		return Progress.PARTIALLY;
	}

	public void markColumnsAsMigrated(String tableName, Set<String> columns) {
		migratedColumns.putAll(tableName, columns);
	}

	public Set<String> getPartiallyMigratedTables() {
		return migratedColumns.keySet().stream()
				.filter(tableName -> getProgress(tableName) == Progress.PARTIALLY)
				.collect(Collectors.toSet());
	}

	public Set<String> getMigratedTables() {
		return migratedColumns.keySet().stream()
				.filter(tableName -> getProgress(tableName) == Progress.DONE)
				.collect(Collectors.toSet());
	}

	public Set<String> getYetToBeMigratedColumns(String tableName) {
		Set<String> migrated = Sets.newHashSet(migratedColumns.get(tableName));
		Table table = catalog.getTable(tableName);

		Set<String> columns = table.getColumns().stream()
				.map(Column::getName)
				.collect(Collectors.toSet());

		return Sets.newHashSet(Sets.difference(columns, migrated));
	}

}
