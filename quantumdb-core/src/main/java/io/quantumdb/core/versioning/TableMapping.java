package io.quantumdb.core.versioning;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.RowSortedTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table.Cell;
import com.google.common.collect.TreeBasedTable;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Table;
import lombok.Data;

public class TableMapping {

	@Data
	static class TableMappingNode {

		TableMappingNode(String tableId, String... basedOnTableIds) {
			this.tableId = tableId;
			this.basedOnTableIds = Sets.newHashSet(basedOnTableIds);
		}

		TableMappingNode(String tableId, Collection<String> basedOnTableIds) {
			this.tableId = tableId;
			this.basedOnTableIds = Sets.newHashSet(basedOnTableIds);
		}

		private final String tableId;
		private final Set<String> basedOnTableIds;
	}

	public static TableMapping bootstrap(Version version, Catalog catalog) {
		TableMapping mapping = new TableMapping();
		for (Table table : catalog.getTables()) {
			mapping.add(version, table.getName(), table.getName());
		}
		return mapping;
	}

	private final RowSortedTable<Version, String, TableMappingNode> versionMapping;

	public TableMapping() {
		this.versionMapping = TreeBasedTable.create();
	}

	public TableMapping copyMappingFromParent(Version version) {
		if (version.getParent() != null && versionMapping.containsRow(version.getParent())) {
			Map<String, TableMappingNode> mapping = versionMapping.row(version.getParent());
			for (Map.Entry<String, TableMappingNode> entry : mapping.entrySet()) {
				putInternally(version, entry.getKey(), entry.getValue());
			}
		}
		return this;
	}

	public TableMapping add(Version version, String tableName, String tableId) {
		putInternally(version, tableName, new TableMappingNode(tableId));
		return this;
	}

	public TableMapping rename(Version version, String oldTableName, String newTableName) {
		TableMappingNode node = removeInternally(version, oldTableName);
		putInternally(version, newTableName, node);
		return this;
	}

	public TableMapping copy(Version version, String sourceTableName, String newTableName, String newTableId) {
		TableMappingNode node = versionMapping.get(version, sourceTableName);
		putInternally(version, newTableName, new TableMappingNode(newTableId, node.getTableId()));
		return this;
	}

	public TableMapping ghost(Version version, String tableName, String newTableId) {
		TableMappingNode node = removeInternally(version, tableName);
		Set<String> basedOnTableIds = Sets.newHashSet(node.getBasedOnTableIds());
		if (basedOnTableIds.isEmpty()) {
			basedOnTableIds.add(node.getTableId());
		}
		putInternally(version, tableName, new TableMappingNode(newTableId, basedOnTableIds));
		return this;
	}

	public Set<Version> getVersions(String tableId) {
		return versionMapping.cellSet().stream()
				.filter(cell -> cell.getValue().getTableId().equals(tableId))
				.map(Cell::getRowKey)
				.collect(Collectors.toSet());
	}

	public String getTableId(Version version, String tableName) {
		String tableId = versionMapping.get(version, tableName).getTableId();
		checkArgument(tableId != null, "Cannot find a tableId for tableId: "
				+ tableName + " at version: " + version);
		return tableId;
	}

	public String getTableName(Version version, String tableId) {
		Map<String, TableMappingNode> mapping = versionMapping.row(version);
		for (Map.Entry<String, TableMappingNode> entry : mapping.entrySet()) {
			if (entry.getValue().getTableId().equals(tableId)) {
				return entry.getKey();
			}
		}
		throw new IllegalArgumentException("Cannot find a tableId for tableId: "
				+ tableId + " at version: " + version + " - mapping was: " + mapping);
	}

	public ImmutableSortedSet<Version> getVersions() {
		return ImmutableSortedSet.copyOf(versionMapping.rowKeySet());
	}

	public ImmutableSet<String> getTableIds() {
		return ImmutableSet.copyOf(versionMapping.values().stream()
				.map(TableMappingNode::getTableId)
				.collect(Collectors.toSet()));
	}

	public ImmutableSet<String> getTableIds(Version version) {
		return ImmutableSet.copyOf(versionMapping.row(version).values().stream()
				.map(TableMappingNode::getTableId)
				.collect(Collectors.toSet()));
	}

	public ImmutableSet<String> getTableNames(Version version) {
		return ImmutableSet.copyOf(versionMapping.row(version).keySet());
	}

	public Optional<String> getTableName(String referencingTableId) {
		return versionMapping.cellSet().stream()
				.filter(entry -> referencingTableId.equals(entry.getValue().getTableId()))
				.map(Cell::getColumnKey)
				.findFirst();
	}

	public Map<String, String> getTableMapping(Version version) {
		Builder<String, String> builder = ImmutableMap.builder();
		getTableIds(version).forEach(tableId -> builder.put(tableId, getTableName(version, tableId)));
		return builder.build();
	}

	public Map<String, String> remove(Version version) {
		Map<String, String> results = versionMapping.row(version).entrySet().stream()
				.collect(Collectors.toMap(entry -> entry.getValue().getTableId(), Entry::getKey));

		results.forEach((tableId, tableName) -> remove(version, tableName));
		return results;
	}

	public String remove(Version version, String tableName) {
		return removeInternally(version, tableName).getTableId();
	}

	public Multimap<String, String> getGhostTableIdMapping(Version source, Version target) {
		Multimap<String, String> ghosts = HashMultimap.create();

		ImmutableSet<String> tableIds = getTableIds(source);
		for (TableMappingNode node : versionMapping.row(target).values()) {
			if (tableIds.contains(node.getTableId())) {
				continue;
			}
			if (node.getBasedOnTableIds() != null && tableIds.containsAll(node.getBasedOnTableIds())) {
				node.getBasedOnTableIds().forEach(tableId -> ghosts.put(tableId, node.getTableId()));
			}
		}

		return ghosts;
	}

	protected TableMapping addInternally(Version version, String tableName, String tableId) {
		versionMapping.put(version, tableName, new TableMappingNode(tableId));
		return this;
	}

	private void putInternally(Version version, String tableName, TableMappingNode node) {
		versionMapping.put(version, tableName, node);
		onPut(version, tableName, node);
	}

	private TableMappingNode removeInternally(Version version, String tableName) {
		TableMappingNode remove = versionMapping.remove(version, tableName);
		onRemove(version, tableName);
		return remove;
	}

	protected void onPut(Version version, String tableName, TableMappingNode node) {
		// Allow for overriding...
	}

	protected void onRemove(Version version, String tableName) {
		// Allow for overriding...
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		getVersions().forEach(version -> {
			builder.append(version.getId() + "\n");
			getTableMapping(version).forEach((tableId, tableName) -> {
				builder.append("\t" + tableId + " -> " + tableName + "\n");
			});
		});
		return builder.toString();
	}

}
