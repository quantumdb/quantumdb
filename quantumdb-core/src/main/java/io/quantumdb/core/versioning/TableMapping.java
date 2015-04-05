package io.quantumdb.core.versioning;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import java.util.Map;
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
				versionMapping.put(version, entry.getKey(), entry.getValue());
			}
		}
		return this;
	}

	public TableMapping add(Version version, String tableName, String tableId) {
		versionMapping.put(version, tableName, new TableMappingNode(tableId));
		return this;
	}

	public TableMapping rename(Version version, String oldTableName, String newTableName) {
		TableMappingNode node = removeInternally(version, oldTableName);
		versionMapping.put(version, newTableName, node);
		return this;
	}

	public TableMapping copy(Version version, String sourceTableName, String newTableName, String newTableId) {
		TableMappingNode node = versionMapping.get(version, sourceTableName);
		versionMapping.put(version, newTableName, new TableMappingNode(newTableId, node.getTableId()));
		return this;
	}

	public TableMapping ghost(Version version, String tableName, String newTableId) {
		TableMappingNode node = removeInternally(version, tableName);
		Set<String> basedOnTableIds = Sets.newHashSet(node.getBasedOnTableIds());
		if (basedOnTableIds.isEmpty()) {
			basedOnTableIds.add(node.getTableId());
		}
		versionMapping.put(version, tableName, new TableMappingNode(newTableId, basedOnTableIds));
		return this;
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

	public String remove(Version version, String tableName) {
		return removeInternally(version, tableName).getTableId();
	}

	TableMappingNode removeInternally(Version version, String tableName) {
		return versionMapping.remove(version, tableName);
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

}
