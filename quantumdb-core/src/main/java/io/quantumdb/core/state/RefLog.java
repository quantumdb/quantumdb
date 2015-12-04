package io.quantumdb.core.state;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.versioning.Version;
import lombok.Data;

public class RefLog {

	@Data
	public static class TableRef {

		private String name;
		private String tableId;
		private final Set<Version> versions;
		private final Map<String, ColumnRef> columns;
		private final Set<SyncRef> outboundSyncs;
		private final Set<SyncRef> inboundSyncs;
		private final RefLog refLog;

		private TableRef(RefLog refLog, String name, String tableId, Version version, Collection<ColumnRef> columns) {
			this.name = name;
			this.tableId = tableId;
			this.refLog = refLog;
			this.versions = Sets.newHashSet(version);
			this.columns = Maps.newHashMap();
			this.outboundSyncs = Sets.newHashSet();
			this.inboundSyncs = Sets.newHashSet();

			columns.forEach(column -> column.table = this);
			refLog.tables.put(version, this);
		}

		public ImmutableSet<SyncRef> getOutboundSyncs() {
			return ImmutableSet.copyOf(outboundSyncs);
		}

		public ImmutableSet<SyncRef> getInboundSync() {
			return ImmutableSet.copyOf(inboundSyncs);
		}

		public ImmutableMap<String, ColumnRef> getColumns() {
			return ImmutableMap.copyOf(columns);
		}

		public Set<TableRef> getBasedOn() {
			return columns.values().stream()
					.flatMap(column -> column.getBasedOn().stream())
					.map(ColumnRef::getTable)
					.distinct()
					.collect(Collectors.toSet());
		}

		public Set<TableRef> getBasisFor() {
			return columns.values().stream()
					.flatMap(column -> column.getBasisFor().stream())
					.map(ColumnRef::getTable)
					.distinct()
					.collect(Collectors.toSet());
		}

		private TableRef fork(Version version) {
			checkArgument(versions.contains(version.getParent()));
			versions.add(version);
			refLog.tables.put(version, this);
			return this;
		}

		public TableRef ghost(String newTableId, Version version) {
			Collection<ColumnRef> newColumns = columns.values().stream()
					.map(ColumnRef::ghost)
					.collect(Collectors.toList());

			return new TableRef(refLog, name, newTableId, version, newColumns);
		}

		public ColumnRef dropColumn(String name) {
			return columns.remove(name);
		}

		public TableRef renameColumn(String oldName, String newName) {
			ColumnRef removed = columns.remove(oldName);
			removed.name = newName;
			columns.put(newName, removed);
			return this;
		}

		public TableRef rename(String newTableName) {
			this.name = newTableName;
			return this;
		}
	}

	@Data
	public static class ColumnRef {

		private final ImmutableSet<ColumnRef> basedOn;
		private final Set<ColumnRef> basisFor;

		private String name;
		private TableRef table;

		public ColumnRef(String name) {
			this(name, Sets.newHashSet());
		}

		public ColumnRef(String name, Collection<ColumnRef> basedOn) {
			this.name = name;
			this.basedOn = ImmutableSet.copyOf(basedOn);
			this.basisFor = Sets.newHashSet();

			basedOn.forEach(column -> column.basisFor.add(this));
		}

		private ColumnRef ghost() {
			return new ColumnRef(name, Sets.newHashSet(this));
		}

		public ImmutableSet<ColumnRef> getBasisFor() {
			return ImmutableSet.copyOf(basisFor);
		}

	}

	@Data
	public static class SyncRef {

		private final String name;
		private final String functionName;
		private final ImmutableMap<ColumnRef, ColumnRef> columnMapping;
		private final TableRef source;
		private final TableRef target;

		private SyncRef(String name, String functionName, Map<ColumnRef, ColumnRef> columnMapping) {
			this.name = name;
			this.functionName = functionName;
			this.columnMapping = ImmutableMap.copyOf(columnMapping);

			List<TableRef> sources = columnMapping.keySet().stream()
					.map(ColumnRef::getTable)
					.distinct()
					.collect(Collectors.toList());

			List<TableRef> targets = columnMapping.values().stream()
					.map(ColumnRef::getTable)
					.distinct()
					.collect(Collectors.toList());

			checkArgument(sources.size() == 1, "There can be only one source table!");
			checkArgument(targets.size() == 1, "There can be only one target table!");

			this.source = sources.get(0);
			this.target = targets.get(0);

			checkArgument(!source.equals(target), "You cannot add a recursive sync function!");

			source.outboundSyncs.add(this);
			target.inboundSyncs.add(this);
		}

	}

	public static RefLog init(Catalog catalog, Version version) {
		RefLog log = new RefLog();
		catalog.getTables().forEach(table -> {
			log.addTable(table.getName(), table.getName(), version, table.getColumns().stream()
					.map(column -> new ColumnRef(column.getName()))
					.collect(Collectors.toList()));
		});
		return log;
	}

	private final Multimap<Version, TableRef> tables;

	public RefLog() {
		this.tables = HashMultimap.create();
	}

	public RefLog prepareFork(Version nextVersion) {
		Version parent = nextVersion.getParent();
		checkArgument(tables.keySet().contains(parent), "You cannot fork to a version whose parent is not in the RefLog!");
		tables.get(parent).forEach(table -> table.fork(nextVersion));
		return this;
	}

	public Collection<TableRef> getTableRefs() {
		return ImmutableSet.copyOf(tables.values());
	}

	public Collection<TableRef> getTableRefs(Version version) {
		return ImmutableSet.copyOf(tables.get(version));
	}

	public TableRef getTableRef(Version version, String tableName) {
		return tables.get(version).stream()
				.filter(table -> table.getName().equals(tableName))
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException("Version: " + version.getId()
						+ " does not contain a TableRef with tableName: " + tableName));
	}

	public TableRef getTableRefById(Version version, String tableId) {
		return tables.get(version).stream()
				.filter(table -> table.getTableId().equals(tableId))
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException("No table with id: " + tableId
						+ " at version: " + version.getId()));
	}

	public TableRef copyTable(Version version, String sourceTableName, String targetTableName, String tableId) {
		TableRef tableRef = dropTable(version, sourceTableName);
		tableRef.tableId = tableId;
		tableRef.name = targetTableName;
		tables.put(version, tableRef);
		return tableRef;
	}

	public TableRef dropTable(Version version, String tableName) {
		TableRef tableRef = getTableRef(version, tableName);
		tables.remove(version, tableRef);
		return tableRef;
	}

	public RefLog addTable(String name, String tableId, Version version, Collection<ColumnRef> columns) {
		long matches = tables.get(version).stream()
				.filter(table -> table.getName().equals(name))
				.count();

		if (matches > 0) {
			throw new IllegalArgumentException("A TableRef for tableName: " + name
					+ " is already present for version: " + version.getId());
		}

		new TableRef(this, name, tableId, version, columns);
		return this;
	}

	public RefLog addSync(String name, String functionName, Map<ColumnRef, ColumnRef> columns) {
		long matches = tables.values().stream()
				.filter(table -> table.getName().equals(name))
				.count();

		if (matches > 0) {
			throw new IllegalArgumentException("A SyncRef with name: " + name + " is already present!");
		}

		new SyncRef(name, functionName, columns);
		return this;
	}

	public Multimap<TableRef, TableRef> getMapping(Version from, Version to) {
		Multimap<TableRef, TableRef> mapping = HashMultimap.create();
		getTableRefs(from).forEach(tableRef -> {
			Version currentVersion = from;
			Set<TableRef> generation = Sets.newHashSet(tableRef);
			while (generation.isEmpty() && !currentVersion.equals(to)) {
				Set<Version> versions = generation.stream()
						.flatMap(ref -> ref.getVersions().stream())
						.distinct()
						.collect(Collectors.toSet());

				checkState(versions.size() == 1, "Generation consists of multiple versions: " + versions);
				currentVersion = versions.iterator().next();

				generation = generation.stream()
						.flatMap(ref -> ref.getBasisFor().stream())
						.collect(Collectors.toSet());
			}

			mapping.putAll(tableRef, generation);
		});
		return mapping;
	}

	public Map<ColumnRef, ColumnRef> getMapping(TableRef from, TableRef to) {

	}

}
