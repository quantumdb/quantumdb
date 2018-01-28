package io.quantumdb.core.versioning;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.quantumdb.core.migration.VersionTraverser;
import io.quantumdb.core.migration.VersionTraverser.Direction;
import io.quantumdb.core.schema.definitions.Catalog;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

@Slf4j
@ToString
@EqualsAndHashCode
public class RefLog {

	@Data
	@ToString(of = { "tableId", "name", "versions", "columns" })
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
			this.versions = Sets.newHashSet();
			this.columns = Maps.newLinkedHashMap();
			this.outboundSyncs = Sets.newHashSet();
			this.inboundSyncs = Sets.newHashSet();

			columns.forEach(column -> {
				column.table = this;
				this.columns.put(column.getName(), column);
			});

			markAsPresent(version);
		}

		public ImmutableSet<SyncRef> getOutboundSyncs() {
			return ImmutableSet.copyOf(outboundSyncs);
		}

		public ImmutableSet<SyncRef> getInboundSyncs() {
			return ImmutableSet.copyOf(inboundSyncs);
		}

		public ImmutableMap<String, ColumnRef> getColumns() {
			return ImmutableMap.copyOf(columns);
		}

		public ColumnRef getColumn(String name) {
			return columns.get(name);
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

		TableRef markAsPresent(Version version) {
			versions.add(version);
			refLog.tables.put(version, this);
			log.debug("Marked TableRef: {} ({}) as present in version: {}", name, tableId, version.getId());
			return this;
		}

		TableRef markAsAbsent(Version version) {
//			checkArgument(versions.contains(version));
			versions.remove(version);
			refLog.tables.remove(version, this);
			log.debug("Marked TableRef: {} ({}) as absent in version: {}", name, tableId, version.getId());
			return this;
		}

		public TableRef ghost(String newTableId, Version version) {
			Collection<ColumnRef> newColumns = columns.values().stream()
					.map(ColumnRef::ghost)
					.collect(Collectors.toList());

			markAsAbsent(version);
			return new TableRef(refLog, name, newTableId, version, newColumns);
		}

		public TableRef addColumn(ColumnRef column) {
			columns.put(column.getName(), column);
			column.setTable(this);
			return this;
		}

		public ColumnRef dropColumn(String name) {
			ColumnRef removed = columns.remove(name);
			removed.drop();
			return removed;
		}

		void drop() {
			columns.forEach((name, ref) -> ref.drop());
			inboundSyncs.forEach(syncRef -> syncRef.getSource().outboundSyncs.remove(syncRef));
			outboundSyncs.forEach(syncRef -> syncRef.getTarget().inboundSyncs.remove(syncRef));
		}

		public TableRef renameColumn(String oldName, String newName) {
			checkState(columns.containsKey(oldName), "Table: " + tableId + " does not contain a column: " + oldName);
			checkState(!columns.containsKey(newName), "Table: " + tableId + " already contains a column: " + newName);

			ColumnRef removed = columns.remove(oldName);
			removed.name = newName;
			columns.put(newName, removed);
			return this;
		}

		public TableRef rename(String newTableName) {
			this.name = newTableName;
			return this;
		}

		public boolean equals(Object other) {
			if (other instanceof TableRef) {
				TableRef otherRef = (TableRef) other;
				return new EqualsBuilder()
						.append(name, otherRef.getName())
						.append(tableId, otherRef.getTableId())
						.append(versions, otherRef.getVersions())
						.append(columns, otherRef.getColumns())
						.append(inboundSyncs, otherRef.getInboundSyncs())
						.append(outboundSyncs, otherRef.getOutboundSyncs())
						.isEquals();
			}
			return false;
		}

		public int hashCode() {
			return new HashCodeBuilder()
					.append(name)
					.append(tableId)
					.toHashCode();
		}
	}

	@Data
	@ToString(of = { "name" })
	public static class ColumnRef {

		private final Set<ColumnRef> basedOn;
		private final Set<ColumnRef> basisFor;

		private String name;
		private TableRef table;

		public ColumnRef(String name) {
			this(name, Sets.newHashSet());
		}

		public ColumnRef(String name, ColumnRef... basedOn) {
			this(name, Sets.newHashSet(basedOn));
		}

		public ColumnRef(String name, Collection<ColumnRef> basedOn) {
			this.name = name;
			this.basedOn = Sets.newHashSet(basedOn);
			this.basisFor = Sets.newHashSet();

			basedOn.forEach(column -> column.basisFor.add(this));
		}

		public ColumnRef ghost() {
			return new ColumnRef(name, Sets.newHashSet(this));
		}

		public ImmutableSet<ColumnRef> getBasisFor() {
			return ImmutableSet.copyOf(basisFor);
		}

		public ImmutableSet<ColumnRef> getBasedOn() {
			return ImmutableSet.copyOf(basedOn);
		}

		void drop() {
			for (ColumnRef from : basedOn) {
				for (ColumnRef to : basisFor) {
					to.basedOn.remove(this);
					to.basedOn.add(from);

					from.basisFor.remove(this);
					from.basisFor.add(to);
				}
			}
		}

		public boolean equals(Object other) {
			if (other instanceof ColumnRef) {
				ColumnRef otherRef = (ColumnRef) other;
				EqualsBuilder builder = new EqualsBuilder()
						.append(name, otherRef.getName());

				if (table != null) {
					builder.append(table.getName(), otherRef.getTable().getName());
				}
				else if (otherRef.getTable() != null) {
					return false;
				}

				return builder.isEquals();
			}
			return false;
		}

		public int hashCode() {
			HashCodeBuilder builder = new HashCodeBuilder()
					.append(name);

			if (table != null) {
				builder.append(table.getName());
			}

			return builder.toHashCode();
		}

	}

	@Data
	@ToString(of = { "name", "functionName", "columnMapping", "source", "target" })
	public static class SyncRef {

		private final String name;
		private final String functionName;
		private final Map<ColumnRef, ColumnRef> columnMapping;
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

			columnMapping.forEach((from, to) -> {
				from.basisFor.add(to);
				to.basedOn.add(from);
			});
		}

		public Direction getDirection() {
			Version origin = VersionTraverser.getFirst(source.getVersions());
			Version destination = VersionTraverser.getFirst(target.getVersions());
			return VersionTraverser.getDirection(origin, destination);
		}

		public ImmutableMap<ColumnRef, ColumnRef> getColumnMapping() {
			return ImmutableMap.copyOf(columnMapping);
		}

		public void addColumnMapping(ColumnRef source, ColumnRef target) {
			columnMapping.put(source, target);
		}

		public void dropColumnMapping(ColumnRef source, ColumnRef target) {
			columnMapping.remove(source, target);
		}

		public void drop() {
			source.outboundSyncs.remove(this);
			target.inboundSyncs.remove(this);
		}

		public boolean equals(Object other) {
			if (other instanceof SyncRef) {
				SyncRef otherRef = (SyncRef) other;
				EqualsBuilder builder = new EqualsBuilder()
						.append(name, otherRef.getName())
						.append(functionName, otherRef.getFunctionName());

				if (source != null) {
					builder.append(source.getName(), otherRef.getSource().getName());
				}
				else if (otherRef.getSource() != null) {
					return false;
				}

				if (target != null) {
					builder.append(target.getName(), otherRef.getTarget().getName());
				}
				else if (otherRef.getTarget() != null) {
					return false;
				}

				return builder.isEquals();
			}
			return false;
		}

		public int hashCode() {
			HashCodeBuilder builder = new HashCodeBuilder()
					.append(name)
					.append(functionName);

			if (source != null) {
				builder.append(source.getName());
			}
			if (target != null) {
				builder.append(target.getName());
			}

			return builder.toHashCode();
		}

	}

	public static RefLog init(Catalog catalog, Version version) {
		return new RefLog().bootstrap(catalog, version);
	}

	private final Multimap<Version, TableRef> tables;
	private final Set<Version> activeVersions;

	/**
	 * Creates a new RefLog object.
	 */
	public RefLog() {
		this.tables = LinkedHashMultimap.create();
		this.activeVersions = Sets.newHashSet();
	}

	/**
	 * Initializes this RefLog object based on the specified Catalog and current Version.
	 *
	 * @param catalog The Catalog describing the current state of the database.
	 * @param version The current version of the database.
	 * @return The constructed RefLog object.
	 */
	public RefLog bootstrap(Catalog catalog, Version version) {
		checkArgument(catalog != null, "You must specify a catalog!");
		checkArgument(version != null && version.getParent() == null, "You must specify a root version!");

		catalog.getTables().forEach(table -> {
			TableRef tableRef = addTable(table.getName(), table.getName(), version, table.getColumns().stream()
					.map(column -> new ColumnRef(column.getName()))
					.collect(Collectors.toList()));

			log.debug("Added TableRef: {} (id: {}) for version: {} with columns: {}", table.getName(),
					table.getName(), version, tableRef.getColumns().keySet());
		});

		// Register version as active.
		setVersionState(version, true);

		return this;
	}

	/**
	 * Creates an internal fork of the RefLog. The fork will be based on the specified version's parent, and contain
	 * all TableRef objects which are also present in the specified version's parent.
	 *
	 * @param version The next Version to fork to.
	 * @return The same RefLog instance.
	 */
	public RefLog fork(Version version) {
		checkArgument(version != null, "You must specify a version!");
		checkArgument(version.getParent() != null, "You cannot fork to a root version!");

		Version parent = version.getParent();
		checkArgument(tables.isEmpty() || tables.keySet().contains(parent),
				"You cannot fork to a version whose parent is not in the RefLog!");

		tables.get(parent).forEach(table -> table.markAsPresent(version));
		return this;
	}

	/**
	 * @return a Collection of TableRef objects currently registered with this RefLog object.
	 */
	public Collection<TableRef> getTableRefs() {
		return ImmutableSet.copyOf(tables.values());
	}

	/**
	 * @return a Collection of TableRef objects currently registered with this RefLog object that are present in
	 * the specified version.
	 */
	public Collection<TableRef> getTableRefs(Version version) {
		checkArgument(version != null, "You must specify a version!");
		return ImmutableSet.copyOf(tables.get(version));
	}

	/**
	 * This method retrieves the TableRef object from the RefLog with the specified table name at the specified
	 * version. If no such TableRef matches these criteria an IllegalArgumentException will be thrown.
	 *
	 * @param version The version in which the TableRef should be present.
	 * @param tableName The name of the table represented by the TableRef.
	 * @return The retrieved TableRef object.
	 * @throws IllegalArgumentException When no TableRef matches the specified criteria.
	 */
	public TableRef getTableRef(Version version, String tableName) {
		checkArgument(version != null, "You must specify a version!");
		checkArgument(!isNullOrEmpty(tableName), "You must specify a table name!");

		return tables.get(version).stream()
				.filter(table -> table.getName().equals(tableName))
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException("Version: " + version.getId()
						+ " does not contain a TableRef with tableName: " + tableName));
	}

	/**
	 * This method retrieves the TableRef object from the RefLog with the specified table ID. If no TableRef
	 * matches the criteria an IllegalArgumentException will be thrown.
	 *
	 * @param tableId The ID of the table represented by the TableRef.
	 * @return The retrieved TableRef object.
	 * @throws IllegalArgumentException When no TableRef matches the specified criteria.
	 */
	public TableRef getTableRefById(String tableId) {
		checkArgument(!isNullOrEmpty(tableId), "You must specify a table ID!");

		return tables.values().stream()
				.filter(table -> table.getTableId().equals(tableId))
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException("No table with id: " + tableId));
	}

	/**
	 * This method replaces an existing TableRef specified through the version, and source table name, drops that
	 * TableRef for that particular version, and creates a new TableRef with the new target table name, and table ID
	 * for that particular version. The columns of the new TableRef will be based off the old TableRef.
	 *
	 * @param version The version at which the replace takes place.
	 * @param sourceTableName The table name of the TableRef to replace.
	 * @param targetTableName The table name of the TableRef which will replace the old TableRef.
	 * @param tableId The table ID of the TableRef which will replace the old TableRef.
	 * @return The created TableRef object.
	 */
	public TableRef replaceTable(Version version, String sourceTableName, String targetTableName, String tableId) {
		checkArgument(version != null, "You must specify a version!");
		checkArgument(!isNullOrEmpty(sourceTableName), "You must specify a source table name!");
		checkArgument(!isNullOrEmpty(targetTableName), "You must specify a target table name!");
		checkArgument(!isNullOrEmpty(tableId), "You must specify a table ID!");

		TableRef tableRef = dropTable(version, sourceTableName);
		return new TableRef(this, targetTableName, tableId, version, tableRef.getColumns().values().stream()
				.map(columnRef -> new ColumnRef(columnRef.getName(), Lists.newArrayList(columnRef)))
				.collect(Collectors.toList()));
	}

	/**
	 * Drops a TableRef at a particular version, with a specific table name. If the TableRef is only connected
	 * to the specified version, it will be disconnected from the RefLog. If the TableRef is connected to multiple
	 * versions it will remain connected to the RefLog.
	 *
	 * @param version The version at which to remove the TableRef from the RefLog.
	 * @param tableName The name of the table which the TableRef represents.
	 * @return The dropped TableRef object.
	 */
	public TableRef dropTable(Version version, String tableName) {
		checkArgument(version != null, "You must specify a version!");
		checkArgument(!isNullOrEmpty(tableName), "You must specify a table name!");

		TableRef tableRef = getTableRef(version, tableName);
		tableRef.versions.remove(version);
		tables.remove(version, tableRef);

		if (tableRef.versions.isEmpty()) {
			tableRef.drop();
		}

		return tableRef;
	}

	/**
	 * Drops a TableRef entirely from the RefLog regardless of which versions it's connected to.
	 *
	 * @param tableRef The TableRef object to drop from the RefLog.
	 */
	public void dropTable(TableRef tableRef) {
		checkArgument(tableRef != null, "You must specify a TableRef!");

		List<Version> versions = tables.entries().stream()
				.filter(entry -> entry.getValue().equals(tableRef))
				.map(Entry::getKey)
				.collect(Collectors.toList());

		versions.forEach(version -> tables.remove(version, tableRef));
		tableRef.drop();
	}

	/**
	 * Creates a new TableRef for the specified table ID, name, and columns at the specified version.
	 *
	 * @param name The name of the table.
	 * @param tableId The table ID of the table.
	 * @param version The version at which this table exists.
	 * @param columns The columns present in the table.
	 * @return The constructed TableRef object.
	 */
	public TableRef addTable(String name, String tableId, Version version, ColumnRef... columns) {
		return addTable(name, tableId, version, Lists.newArrayList(columns));
	}

	/**
	 * Creates a new TableRef for the specified table ID, name, and columns at the specified version.
	 *
	 * @param name The name of the table.
	 * @param tableId The table ID of the table.
	 * @param version The version at which this table exists.
	 * @param columns The columns present in the table.
	 * @return The constructed TableRef object.
	 */
	public TableRef addTable(String name, String tableId, Version version, Collection<ColumnRef> columns) {
		checkArgument(!isNullOrEmpty(name), "You must specify a 'name'!");
		checkArgument(!isNullOrEmpty(tableId), "You must specify a 'tableId'!");
		checkArgument(version != null, "You must specify a 'version'!");
		checkArgument(columns != null, "You must specify a collection of 'columns'!");

		long matches = tables.get(version).stream()
				.filter(table -> table.getName().equals(name))
				.count();

		if (matches > 0) {
			throw new IllegalStateException("A TableRef for tableName: " + name
					+ " is already present for version: " + version.getId());
		}

		return new TableRef(this, name, tableId, version, columns);
	}

	/**
	 * Defines that there's a trigger and function which manage the synchronization between two different tables
	 * in one particular direction.
	 *
	 * @param name The name of the trigger.
	 * @param functionName The name of the function.
	 * @param columns The column mapping from the source table, to the target table.
	 * @return The constructed SyncRef object.
	 */
	public SyncRef addSync(String name, String functionName, Map<ColumnRef, ColumnRef> columns) {
		long matches = tables.values().stream()
				.filter(table -> table.getName().equals(name))
				.count();

		if (matches > 0) {
			throw new IllegalArgumentException("A SyncRef with name: " + name + " is already present!");
		}

		return new SyncRef(name, functionName, columns);
	}


	public Multimap<TableRef, TableRef> getTableMapping(Version from, Version to) {
		return getTableMapping(from, to, true);
	}

	/**
	 * Returns a Multimap defining the evolutionary relation between TableRefs in the specified 'from' version,
	 * to TableRefs in specified the 'to' version.
	 *
	 * @param from The starting version.
	 * @param to The final version.
	 * @return The mapping between TableRefs between these two versions.
	 */
	public Multimap<TableRef, TableRef> getTableMapping(Version from, Version to, boolean filterUnchanged) {
		Multimap<TableRef, TableRef> mapping = HashMultimap.create();
		getTableRefs(from).forEach(tableRef -> {
			Set<TableRef> targets = Sets.newHashSet();
			List<TableRef> toCheck = Lists.newLinkedList();
			toCheck.add(tableRef);

			while (!toCheck.isEmpty()) {
				TableRef pointer = toCheck.remove(0);
				if (pointer.getVersions().contains(to)) {
					if (!filterUnchanged || !pointer.getTableId().equals(tableRef.getTableId())) {
						targets.add(pointer);
					}
				}
				else {
					toCheck.addAll(pointer.getBasisFor());
				}
			}

			mapping.putAll(tableRef, targets);
		});
		return mapping;
	}

	/**
	 * This method returns a Map which defines the evolutionary relation between ColumnRefs in the specified
	 * 'from' TableRef, to the specified 'to' TableRef.
	 *
	 * @param from The source TableRef.
	 * @param to The target TableRef.
	 * @return The column mapping between the two TableRefs.
	 */
	public Map<ColumnRef, ColumnRef> getColumnMapping(TableRef from, TableRef to) {
		boolean forwards = isForwards(from, to);

		Multimap<ColumnRef, ColumnRef> mapping = HashMultimap.create();
		from.getColumns().forEach((k, v) -> mapping.put(v, v));

		while (true) {
			Multimap<ColumnRef, ColumnRef> pending = HashMultimap.create();
			for (ColumnRef source : mapping.keySet()) {
				Collection<ColumnRef> targets = mapping.get(source);
				targets.stream()
						.filter(target -> {
							TableRef table = target.getTable();
							return !table.equals(to);
						})
						.forEach(target -> pending.put(source, target));
			}

			if (pending.isEmpty()) {
				break;
			}

			pending.entries().forEach(entry -> {
				ColumnRef columnRef = entry.getValue();
				mapping.remove(entry.getKey(), columnRef);

				Set<ColumnRef> nextColumns = null;
				if (forwards) {
					nextColumns = columnRef.getBasisFor();
				}
				else {
					nextColumns = columnRef.getBasedOn();
				}

				if (!nextColumns.isEmpty()) {
					mapping.putAll(entry.getKey(), nextColumns);
				}
			});
		}

		return mapping.entries().stream()
				.collect(Collectors.toMap(Entry::getKey, Entry::getValue));
	}

	private boolean isForwards(TableRef from, TableRef to) {
		boolean forwards = false;
		if (!from.equals(to)) {
//			List<TableRef> toDo = Lists.newLinkedList(from.getBasisFor());
//			while (!toDo.isEmpty()) {
//				TableRef current = toDo.remove(0);
//				if (current.equals(to)) {
//					forwards = true;
//				}
//			}
			Version origin = VersionTraverser.getFirst(from.getVersions());
			Version target = VersionTraverser.getFirst(to.getVersions());
			forwards = VersionTraverser.getDirection(origin, target) == Direction.FORWARDS;
		}
		return forwards;
	}

	public void setVersionState(Version version, boolean active) {
		if (active) {
			activeVersions.add(version);
		}
		else {
			activeVersions.remove(version);
		}
	}

	/**
	 * @return An ImmutableSet of Versions covered by this RefLog.
	 */
	public ImmutableSet<Version> getVersions() {
		return ImmutableSet.copyOf(activeVersions);
	}

}
