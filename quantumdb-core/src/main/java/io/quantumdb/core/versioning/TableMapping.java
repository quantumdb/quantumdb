package io.quantumdb.core.versioning;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.RowSortedTable;
import com.google.common.collect.TreeBasedTable;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Table;

public class TableMapping {

	public static TableMapping bootstrap(Version version, Catalog catalog) {
		TableMapping mapping = new TableMapping();
		for (Table table : catalog.getTables()) {
			mapping.set(version, table.getName(), table.getName());
		}
		return mapping;
	}

	private final RowSortedTable<Version, String, String> versionMapping;

	public TableMapping() {
		this.versionMapping = TreeBasedTable.create();
	}

	public TableMapping copyMappingFromParent(Version version) {
		if (version.getParent() != null && versionMapping.containsRow(version.getParent())) {
			Map<String, String> mapping = versionMapping.row(version.getParent());
			for (Map.Entry<String, String> entry : mapping.entrySet()) {
				versionMapping.put(version, entry.getKey(), entry.getValue());
			}
		}
		return this;
	}

	public TableMapping set(Version version, String tableName, String alias) {
		versionMapping.put(version, tableName, alias);
		return this;
	}

	public String getTableId(Version version, String tableName) {
		String tableId = versionMapping.get(version, tableName);
		checkArgument(tableId != null, "Cannot find a tableId for tableName: "
				+ tableName + " at version: " + version);
		return tableId;
	}

	public String getTableName(Version version, String tableId) {
		Map<String, String> mapping = versionMapping.row(version);
		for (Map.Entry<String, String> entry : mapping.entrySet()) {
			if (entry.getValue().equals(tableId)) {
				return entry.getKey();
			}
		}
		throw new IllegalArgumentException("Cannot find a tableName for tableId: "
				+ tableId + " at version: " + version);
	}

	public ImmutableSortedSet<Version> getVersions() {
		return ImmutableSortedSet.copyOf(versionMapping.rowKeySet());
	}

	public ImmutableSet<String> getTableIds() {
		return ImmutableSet.copyOf(versionMapping.values());
	}

	public ImmutableSet<String> getTableIds(Version version) {
		return ImmutableSet.copyOf(versionMapping.row(version).values());
	}

	public ImmutableSet<String> getTableNames(Version version) {
		return ImmutableSet.copyOf(versionMapping.row(version).keySet());
	}

	public String remove(Version version, String tableName) {
		return versionMapping.remove(version, tableName);
	}

}
