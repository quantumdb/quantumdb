package io.quantumdb.core.versioning;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.operations.SchemaOperation;
import io.quantumdb.core.utils.RandomHasher;
import io.quantumdb.core.versioning.RefLog.ColumnRef;
import io.quantumdb.core.versioning.RefLog.SyncRef;
import io.quantumdb.core.versioning.RefLog.TableRef;
import lombok.Data;

public class Backend {

	@Data
	private static class TableId {
		private final String tableId;
		private final String tableName;
	}

	@Data
	private static class TableColumn {
		private final long id;
		private final TableId table;
		private final String column;
	}

	@Data
	private static class RawTableColumn {
		private final long id;
		private final String table;
		private final String column;
	}

	@Data
	private static class RawColumn {
		private final String table;
		private final String column;
	}

	@Data
	private static class RawColumnMapping {
		private final long id;
		private final RawColumn source;
		private final RawColumn target;
	}

	@Data
	private static class TableColumnMapping {
		private final long id;
		private final TableColumn source;
		private final TableColumn target;
	}

	@Data
	private static class RawChangelogEntry {
		private final String versionId;
		private final String schemaOperation;
		private final String parentVersionId;
	}

	@Data
	private static class RawChangeSet {
		private final String versionId;
		private final String description;
		private final String author;
		private final Date created;
	}

	private final Gson gson;

	public Backend() {
		this.gson = new Gson();
	}

	public State load(Connection connection, Catalog catalog) throws SQLException {
		Changelog changelog = loadChangelog(connection);
		Map<String, TableId> tableIds = listTableIds(connection);
		Multimap<Version, TableId> tableVersions = listTableVersions(connection, tableIds, changelog);
		List<TableColumn> tableColumns = listTableColumns(connection, tableIds);
		List<TableColumnMapping> columnMappings = listTableColumnMappings(connection, tableColumns);

		Multimap<TableId, TableColumn> columnsPerTable = LinkedHashMultimap.create();
		tableColumns.forEach(column -> columnsPerTable.put(column.getTable(), column));

		Map<TableColumn, ColumnRef> columnCache = Maps.newLinkedHashMap();

		RefLog refLog = new RefLog();
		List<Version> toDo = Lists.newLinkedList();
		toDo.add(changelog.getRoot());

		while (!toDo.isEmpty()) {
			Version version = toDo.remove(0);
			for (TableId table : tableVersions.get(version)) {
				TableRef tableRef = refLog.getTableRefs(version).stream()
						.filter(ref -> ref.getName().equals(table.getTableName()))
						.findFirst()
						.orElse(null);

				if (tableRef != null) {
					tableRef.markAsPresent(version);
				}
				else {
					Map<TableColumn, ColumnRef> columnRefs = columnsPerTable.get(table).stream()
							.collect(Collectors.toMap(Function.identity(), column -> {
								List<ColumnRef> basedOn = columnMappings.stream()
										.filter(mapping -> mapping.getTarget().equals(column))
										.map(TableColumnMapping::getSource)
										.map(columnCache::get)
										.filter(ref -> ref != null) // TODO: Really needed?
										.collect(Collectors.toList());

								return new ColumnRef(column.getColumn(), basedOn);
							}, (l, r) -> l, LinkedHashMap::new));

					columnCache.putAll(columnRefs);
					refLog.addTable(table.getTableName(), table.getTableId(), version, columnRefs.values());
				}
			}

			if (version.getChild() != null) {
				toDo.add(version.getChild());
			}
		}

		addSynchronizers(connection, refLog, columnMappings);

		return new State(catalog, refLog, changelog);
	}

	public void persist(Connection connection, State state) throws SQLException {
		persistChangelog(connection, state.getChangelog());

		RefLog refLog = state.getRefLog();
		persistTables(connection, refLog);
		persistTableVersions(connection, refLog);
		Collection<RawTableColumn> columns = persistTableColumns(connection, refLog);
		Map<Long, RawColumnMapping> columnMapping = persistColumnMappings(connection, refLog, columns);
		Map<Long, SyncRef> syncRefs = persistTableSynchronizers(connection, refLog);
		persistSynchronizerColumns(connection, refLog, syncRefs, columnMapping);
	}

	private void persistChangelog(Connection connection, Changelog changelog) throws SQLException {
		persistChangelogEntries(connection, changelog);
		persistChangesets(connection, changelog);
	}

	private void persistChangelogEntries(Connection connection, Changelog changelog) throws SQLException {
		Map<String, Version> mapping = Maps.newHashMap();
		List<Version> versions = Lists.newLinkedList();
		versions.add(changelog.getRoot());

		while (!versions.isEmpty()) {
			Version version = versions.remove(0);
			mapping.put(version.getId(), version);
			if (version.getChild() != null) {
				versions.add(version.getChild());
			}
		}

		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb_changelog;";
			String deleteQuery = "DELETE FROM quantumdb_changelog WHERE version_id = ?;";
			String insertQuery = "INSERT INTO quantumdb_changelog (version_id, schema_operation, parent_version_id) VALUES (?, ?, ?);";
			String updateQuery = "UPDATE quantumdb_changelog SET schema_operation = ?, parent_version_id = ? WHERE version_id = ?;";

			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				String versionId = resultSet.getString("version_id");
				Version version = mapping.remove(versionId);
				if (version != null) {
					try (PreparedStatement update = connection.prepareStatement(updateQuery)) {
						update.setString(1, gson.toJson(version.getSchemaOperation()));
						if (version.getParent() == null) {
							update.setString(2, null);
						}
						else {
							update.setString(2, version.getParent().getId());
						}
						update.setString(3, versionId);
						update.execute();
					}
				}
				else {
					try (PreparedStatement delete = connection.prepareStatement(deleteQuery)) {
						delete.setString(1, versionId);
						delete.execute();
					}
				}
			}

			if (!mapping.isEmpty()) {
				PreparedStatement insert = connection.prepareStatement(insertQuery);
				for (Entry<String, Version> entry : mapping.entrySet()) {
					Version version = entry.getValue();
					insert.setString(1, entry.getKey());
					insert.setString(2, gson.toJson(version.getSchemaOperation()));

					if (version.getParent() == null) {
						insert.setString(3, null);
					}
					else {
						insert.setString(3, version.getParent().getId());
					}
					insert.execute();
				}
			}

			resultSet.close();
		}
	}

	private void persistChangesets(Connection connection, Changelog changelog) throws SQLException {
		Map<String, ChangeSet> mapping = Maps.newHashMap();
		List<Version> versions = Lists.newLinkedList();
		versions.add(changelog.getRoot());

		while (!versions.isEmpty()) {
			Version version = versions.remove(0);
			mapping.put(version.getId(), version.getChangeSet());
			if (version.getChild() != null) {
				versions.add(version.getChild());
			}
		}

		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb_changesets;";
			String deleteQuery = "DELETE FROM quantumdb_changesets WHERE version_id = ?;";
			String insertQuery = "INSERT INTO quantumdb_changesets (version_id, author, description, created) VALUES (?, ?, ?, ?);";
			String updateQuery = "UPDATE quantumdb_changesets SET author = ?, description = ?, created = ? WHERE version_id = ?;";

			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				String versionId = resultSet.getString("version_id");
				ChangeSet changeSet = mapping.remove(versionId);
				if (changeSet != null) {
					try (PreparedStatement update = connection.prepareStatement(updateQuery)) {
						update.setString(1, changeSet.getAuthor());
						update.setString(2, changeSet.getDescription());
						update.setTimestamp(3, new Timestamp(changeSet.getCreated().getTime()));
						update.setString(4, versionId);
						update.execute();
					}
				}
				else {
					try (PreparedStatement delete = connection.prepareStatement(deleteQuery)) {
						delete.setString(1, versionId);
						delete.execute();
					}
				}
			}

			if (!mapping.isEmpty()) {
				PreparedStatement insert = connection.prepareStatement(insertQuery);
				for (Entry<String, ChangeSet> entry : mapping.entrySet()) {
					insert.setString(1, entry.getKey());

					ChangeSet changeSet = entry.getValue();
					if (changeSet != null) {
						insert.setString(2, changeSet.getAuthor());
						insert.setString(3, changeSet.getDescription());
						insert.setTimestamp(4, new Timestamp(changeSet.getCreated().getTime()));
					}
					else {
						insert.setString(2, "");
						insert.setString(3, "");
						insert.setTimestamp(4, Timestamp.from(Instant.now()));
					}
					insert.execute();
				}
			}

			resultSet.close();
		}
	}

	private void persistTables(Connection connection, RefLog refLog) throws SQLException {
		Map<String, String> tableNameMapping = refLog.getTableRefs().stream()
				.collect(Collectors.toMap(TableRef::getTableId, TableRef::getName));

		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb_tables ORDER BY table_name ASC;";
			String deleteQuery = "DELETE FROM quantumdb_tables WHERE table_id = ?;";
			String insertQuery = "INSERT INTO quantumdb_tables (table_id, table_name) VALUES (?, ?);";
			String updateQuery = "UPDATE quantumdb_tables SET table_name = ? WHERE table_id = ?;";

			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				String tableId = resultSet.getString("table_id");
				if (tableNameMapping.containsKey(tableId)) {
					String tableName = tableNameMapping.remove(tableId);
					if (!resultSet.getString("table_name").equals(tableName)) {
						try (PreparedStatement update = connection.prepareStatement(updateQuery)) {
							update.setString(1, tableName);
							update.setString(2, tableId);
							update.execute();
						}
					}
				}
				else {
					try (PreparedStatement delete = connection.prepareStatement(deleteQuery)) {
						delete.setString(1, tableId);
						delete.execute();
					}
				}
			}

			if (!tableNameMapping.isEmpty()) {
				PreparedStatement insert = connection.prepareStatement(insertQuery);
				for (Entry<String, String> entry : tableNameMapping.entrySet()) {
					insert.setString(1, entry.getKey());
					insert.setString(2, entry.getValue());
					insert.execute();
				}
			}

			resultSet.close();
		}
	}

	private void persistTableVersions(Connection connection, RefLog refLog) throws SQLException {
		Multimap<String, String> versionMapping = HashMultimap.create();
		refLog.getTableRefs()
				.forEach(tableRef -> versionMapping.putAll(tableRef.getTableId(), tableRef.getVersions().stream()
						.map(Version::getId)
						.collect(Collectors.toSet())));

		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb_table_versions ORDER BY table_id ASC;";
			String deleteQuery = "DELETE FROM quantumdb_table_versions WHERE table_id = ? AND version_id = ?;";
			String insertQuery = "INSERT INTO quantumdb_table_versions (table_id, version_id) VALUES (?, ?);";

			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				String tableId = resultSet.getString("table_id");
				String versionId = resultSet.getString("version_id");

				if (versionMapping.containsKey(tableId)) {
					Collection<String> versions = versionMapping.get(tableId);
					if (!versions.contains(versionId)) {
						try (PreparedStatement delete = connection.prepareStatement(deleteQuery)) {
							delete.setString(1, tableId);
							delete.setString(2, versionId);
							delete.execute();
						}
					}
					else {
						versionMapping.remove(tableId, versionId);
					}
				}
				else {
					try (PreparedStatement delete = connection.prepareStatement(deleteQuery)) {
						delete.setString(1, tableId);
						delete.setString(2, versionId);
						delete.execute();
					}
				}
			}

			if (!versionMapping.isEmpty()) {
				PreparedStatement insert = connection.prepareStatement(insertQuery);
				for (Entry<String, String> entry : versionMapping.entries()) {
					insert.setString(1, entry.getKey());
					insert.setString(2, entry.getValue());
					insert.execute();
				}
			}

			resultSet.close();
		}
	}

	private Collection<RawTableColumn> persistTableColumns(Connection connection, RefLog refLog) throws SQLException {
		Multimap<String, String> columnMapping = LinkedHashMultimap.create();
		refLog.getTableRefs()
				.forEach(tableRef -> columnMapping.putAll(tableRef.getTableId(), tableRef.getColumns().keySet()));

		List<RawTableColumn> columns = Lists.newArrayList();
		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb_table_columns ORDER BY id ASC;";
			String deleteQuery = "DELETE FROM quantumdb_table_columns WHERE id = ?;";
			String insertQuery = "INSERT INTO quantumdb_table_columns (table_id, column_name) VALUES (?, ?) RETURNING id;";

			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				Long id = resultSet.getLong("id");
				String tableId = resultSet.getString("table_id");
				if (columnMapping.containsKey(tableId)) {
					Collection<String> columnNames = columnMapping.get(tableId);
					String columnName = resultSet.getString("column_name");
					if (!columnNames.contains(columnName)) {
						try (PreparedStatement delete = connection.prepareStatement(deleteQuery)) {
							delete.setLong(1, id);
							delete.execute();
						}
					}
					else {
						columnMapping.remove(tableId, columnName);
						columns.add(new RawTableColumn(id, tableId, columnName));
					}
				}
				else {
					try (PreparedStatement delete = connection.prepareStatement(deleteQuery)) {
						delete.setLong(1, id);
						delete.execute();
					}
				}
			}

			if (!columnMapping.isEmpty()) {
				try (PreparedStatement insert = connection.prepareStatement(insertQuery)) {
					for (Entry<String, String> entry : columnMapping.entries()) {
						insert.setString(1, entry.getKey());
						insert.setString(2, entry.getValue());
						ResultSet generatedKeys = insert.executeQuery();
						generatedKeys.next();

						long id = generatedKeys.getLong("id");
						columns.add(new RawTableColumn(id, entry.getKey(), entry.getValue()));
					}
				}
			}

			resultSet.close();
		}
		return columns;
	}

	private Map<Long, RawColumnMapping> persistColumnMappings(Connection connection, RefLog refLog, Collection<RawTableColumn> columns) throws SQLException {
		Map<Long, RawColumn> index = columns.stream()
				.collect(Collectors.toMap(RawTableColumn::getId, column -> new RawColumn(column.getTable(), column.getColumn())));

		Map<RawColumn, Long> reverseIndex = columns.stream()
				.collect(Collectors.toMap(column -> new RawColumn(column.getTable(), column.getColumn()), RawTableColumn::getId));

		Multimap<RawColumn, RawColumn> columnMapping = HashMultimap.create();
		for (TableRef tableRef : refLog.getTableRefs()) {
			String tableId = tableRef.getTableId();
			for (ColumnRef columnRef : tableRef.getColumns().values()) {
				String columnName = columnRef.getName();
				RawColumn target = new RawColumn(tableId, columnName);

				for (ColumnRef basedOn : columnRef.getBasedOn()) {
					String sourceTableId = basedOn.getTable().getTableId();
					String sourceColumnName = basedOn.getName();
					RawColumn source = new RawColumn(sourceTableId, sourceColumnName);

					columnMapping.put(source, target);
				}
			}
		}

		Map<Long, RawColumnMapping> results = Maps.newHashMap();
		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb_column_mappings;";
			String deleteQuery = "DELETE FROM quantumdb_column_mappings WHERE id = ?;";
			String insertQuery = "INSERT INTO quantumdb_column_mappings (source_column_id, target_column_id) VALUES (?, ?) RETURNING id;";

			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				long id = resultSet.getLong("id");
				long sourceId = resultSet.getLong("source_column_id");
				long targetId = resultSet.getLong("target_column_id");

				RawColumn source = index.get(sourceId);
				RawColumn target = index.get(targetId);

				if (columnMapping.containsEntry(source, target)) {
					columnMapping.remove(source, target);
					results.put(id, new RawColumnMapping(id, source, target));
				}
				else {
					try (PreparedStatement delete = connection.prepareStatement(deleteQuery)) {
						delete.setLong(1, id);
						delete.execute();
					}
				}
			}

			if (!columnMapping.isEmpty()) {
				try (PreparedStatement insert = connection.prepareStatement(insertQuery)) {
					for (Entry<RawColumn, RawColumn> entry : columnMapping.entries()) {
						long sourceId = reverseIndex.get(entry.getKey());
						long targetId = reverseIndex.get(entry.getValue());

						insert.setLong(1, sourceId);
						insert.setLong(2, targetId);
						ResultSet generatedKeys = insert.executeQuery();
						generatedKeys.next();

						long id = generatedKeys.getLong("id");
						results.put(id, new RawColumnMapping(id, entry.getKey(), entry.getValue()));
					}
				}
			}
			resultSet.close();
		}

		return results;
	}

	private Map<Long, SyncRef> persistTableSynchronizers(Connection connection, RefLog refLog) throws SQLException {
		Table<String, String, SyncRef> syncMapping = HashBasedTable.create();
		for (TableRef tableRef : refLog.getTableRefs()) {
			for (SyncRef syncRef : tableRef.getInboundSyncs()) {
				String sourceId = syncRef.getSource().getTableId();
				String targetId = syncRef.getTarget().getTableId();
				syncMapping.put(sourceId, targetId, syncRef);
			}
		}

		Map<Long, SyncRef> mapping = Maps.newHashMap();
		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb_synchronizers;";
			String deleteQuery = "DELETE FROM quantumdb_synchronizers WHERE id = ?;";
			String insertQuery = "INSERT INTO quantumdb_synchronizers (source_table_id, target_table_id, trigger_name, function_name) VALUES (?, ?, ?, ?) RETURNING id;";
			String updateQuery = "UPDATE quantumdb_synchronizers SET trigger_name = ?, function_name = ? WHERE id = ?;";

			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				long id = resultSet.getLong("id");
				String sourceTableId = resultSet.getString("source_table_id");
				String targetTableId = resultSet.getString("target_table_id");

				if (syncMapping.contains(sourceTableId, targetTableId)) {
					SyncRef syncRef = syncMapping.remove(sourceTableId, targetTableId);

					try (PreparedStatement update = connection.prepareStatement(updateQuery)) {
						update.setString(1, syncRef.getName());
						update.setString(2, syncRef.getFunctionName());
						update.setLong(3, id);
						update.execute();
					}

					mapping.put(id, syncRef);
				}
				else {
					try (PreparedStatement delete = connection.prepareStatement(deleteQuery)) {
						delete.setLong(1, id);
						delete.execute();
					}
				}
			}

			if (!syncMapping.isEmpty()) {
				PreparedStatement insert = connection.prepareStatement(insertQuery);
				for (SyncRef syncRef : syncMapping.values()) {
					insert.setString(1, syncRef.getSource().getTableId());
					insert.setString(2, syncRef.getTarget().getTableId());
					insert.setString(3, syncRef.getName());
					insert.setString(4, syncRef.getFunctionName());
					ResultSet generatedKeys = insert.executeQuery();

					generatedKeys.next();
					long id = generatedKeys.getLong("id");
					mapping.put(id, syncRef);
				}
			}

			resultSet.close();
		}
		return mapping;
	}

	private void persistSynchronizerColumns(Connection connection, RefLog refLog, Map<Long, SyncRef> syncRefs,
			Map<Long, RawColumnMapping> columnMapping) throws SQLException {

		Table<String, String, Long> syncIndex = HashBasedTable.create();
		syncRefs.forEach((id, ref) -> syncIndex.put(ref.getSource().getTableId(), ref.getTarget().getTableId(), id));

		Multimap<Long, Long> idMapping = HashMultimap.create();
		columnMapping.forEach((id, mapping) -> {
			String sourceTableId = mapping.getSource().getTable();
			String targetTableId = mapping.getTarget().getTable();
			long syncId = syncIndex.get(sourceTableId, targetTableId);
			idMapping.put(syncId, mapping.getId());
		});

		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb_synchronizer_columns;";
			String deleteQuery = "DELETE FROM quantumdb_synchronizer_columns WHERE synchronizer_id = ? AND column_mapping = ?;";
			String insertQuery = "INSERT INTO quantumdb_synchronizer_columns (synchronizer_id, column_mapping_id) VALUES (?, ?);";

			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				long synchronizerId = resultSet.getLong("synchronizer_id");
				long columnMappingId = resultSet.getLong("column_mapping_id");

				if (idMapping.containsEntry(synchronizerId, columnMappingId)) {
					idMapping.remove(synchronizerId, columnMappingId);
				}
				else {
					try (PreparedStatement delete = connection.prepareStatement(deleteQuery)) {
						delete.setLong(1, synchronizerId);
						delete.setLong(2, columnMappingId);
						delete.execute();
					}
				}
			}

			if (!idMapping.isEmpty()) {
				PreparedStatement insert = connection.prepareStatement(insertQuery);
				for (Entry<Long, Long> idEntry : idMapping.entries()) {
					insert.setLong(1, idEntry.getKey());
					insert.setLong(2, idEntry.getValue());
					insert.execute();
				}
			}

			resultSet.close();
		}
	}

	private Changelog loadChangelog(Connection connection) throws SQLException {
		List<RawChangelogEntry> entries = loadChangelogEntries(connection);
		Map<String, RawChangeSet> changeSets = loadChangesets(connection);

		List<RawChangelogEntry> changeSetContents = Lists.newArrayList();
		Changelog changelog = new Changelog(entries.remove(0).getVersionId());
		while (!entries.isEmpty()) {
			RawChangelogEntry entry = entries.remove(0);
			boolean finalizeChangeSet = changeSets.containsKey(entry.getVersionId());

			if (entries.isEmpty()) {
				changeSetContents.add(entry);
				finalizeChangeSet = true;
			}

			if (finalizeChangeSet) {
				RawChangelogEntry first = changeSetContents.get(0);
				RawChangeSet rawChangeSet = changeSets.get(first.getVersionId());
				ChangeSet changeSet = new ChangeSet(rawChangeSet.getAuthor(), rawChangeSet.getCreated(), rawChangeSet.getDescription());

				for (RawChangelogEntry entryInSet : changeSetContents) {
					String parentVersionId = entryInSet.getParentVersionId();
					Version parentVersion = changelog.getVersion(parentVersionId);
					SchemaOperation schemaOperation = gson.fromJson(entryInSet.getSchemaOperation(), SchemaOperation.class);
					changelog.addChangeSet(parentVersion, entryInSet.getVersionId(), changeSet, schemaOperation);
				}
				changeSetContents.clear();
			}
			else {
				changeSetContents.add(entry);
			}
		}
		return changelog;
	}

	private List<RawChangelogEntry> loadChangelogEntries(Connection connection) throws SQLException {
		RawChangelogEntry root = null;
		Multimap<String, RawChangelogEntry> entries = HashMultimap.create();
		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb_changelog;";
			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				String versionId = resultSet.getString("version_id");
				String schemaOperation = resultSet.getString("schema_operation");
				String parentVersionId = resultSet.getString("parent_version_id");

				RawChangelogEntry entry = new RawChangelogEntry(versionId, schemaOperation, parentVersionId);
				if (parentVersionId == null) {
					root = entry;
				}
				else {
					entries.put(parentVersionId, entry);
				}
			}

			resultSet.close();
		}

		if (root == null) {
			root = new RawChangelogEntry(RandomHasher.generateHash(), null, null);
		}

		List<RawChangelogEntry> pointer = Lists.newArrayList(root);
		List<RawChangelogEntry> sorted = Lists.newArrayList();
		while (!pointer.isEmpty()) {
			RawChangelogEntry entry = pointer.remove(0);
			sorted.add(entry);

			String versionId = entry.getVersionId();
			pointer.addAll(entries.get(versionId));
		}

		return sorted;
	}

	private Map<String, RawChangeSet> loadChangesets(Connection connection) throws SQLException {
		Map<String, RawChangeSet> changeSets = Maps.newHashMap();
		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb_changesets;";
			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				String versionId = resultSet.getString("version_id");
				String description = resultSet.getString("description");
				String author = resultSet.getString("author");
				Date created = resultSet.getTimestamp("created");

				RawChangeSet changeset = new RawChangeSet(versionId, description, author, created);
				changeSets.put(versionId, changeset);

			}
			resultSet.close();
		}

		return changeSets;
	}

	private Map<String, TableId> listTableIds(Connection connection) throws SQLException {
		Map<String, TableId> mapping = Maps.newLinkedHashMap();
		try (Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery("SELECT * FROM quantumdb_tables ORDER BY table_name ASC;");
			while (resultSet.next()) {
				String tableId = resultSet.getString("table_id");
				String tableName = resultSet.getString("table_name");
				mapping.put(tableId, new TableId(tableId, tableName));
			}
		}
		return mapping;
	}

	private Multimap<Version, TableId> listTableVersions(Connection connection, Map<String, TableId> tableIds, Changelog changelog) throws SQLException {
		Multimap<Version, TableId> mapping = LinkedHashMultimap.create();
		try (Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery("SELECT * FROM quantumdb_table_versions ORDER BY table_id ASC;");
			while (resultSet.next()) {
				String tableId = resultSet.getString("table_id");
				String versionId = resultSet.getString("version_id");
				TableId table = tableIds.get(tableId);
				Version version = changelog.getVersion(versionId);
				mapping.put(version, table);
			}
		}
		return mapping;
	}

	private List<TableColumn> listTableColumns(Connection connection, Map<String, TableId> tableIds) throws SQLException {
		List<TableColumn> results = Lists.newArrayList();
		try (Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery("SELECT * FROM quantumdb_table_columns ORDER BY id ASC;");
			while (resultSet.next()) {
				long id = resultSet.getLong("id");
				String tableId = resultSet.getString("table_id");
				String column = resultSet.getString("column_name");
				TableId table = tableIds.get(tableId);
				results.add(new TableColumn(id, table, column));
			}
		}
		return results;
	}

	private List<TableColumnMapping> listTableColumnMappings(Connection connection, List<TableColumn> tableColumns) throws SQLException {
		Map<Long, TableColumn> columnById = tableColumns.stream()
				.collect(Collectors.toMap(TableColumn::getId, Function.identity()));

		List<TableColumnMapping> results = Lists.newArrayList();
		try (Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery("SELECT * FROM quantumdb_column_mappings;");
			while (resultSet.next()) {
				long id = resultSet.getLong("id");
				long sourceColumnId = resultSet.getLong("source_column_id");
				long targetColumnId = resultSet.getLong("target_column_id");
				TableColumn source = columnById.get(sourceColumnId);
				TableColumn target = columnById.get(targetColumnId);
				results.add(new TableColumnMapping(id, source, target));
			}
		}
		return results;
	}

	private void addSynchronizers(Connection connection, RefLog refLog, List<TableColumnMapping> columnMappings) throws SQLException {
		Map<Long, TableColumnMapping> columnMapping = columnMappings.stream()
				.collect(Collectors.toMap(TableColumnMapping::getId, Function.identity()));

		Multimap<Long, Long> syncColumMappings = HashMultimap.create();
		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb_synchronizer_columns;";

			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				long synchronizerId = resultSet.getLong("synchronizer_id");
				long columnMappingId = resultSet.getLong("column_mapping_id");
				syncColumMappings.put(synchronizerId, columnMappingId);
			}
		}

		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb_synchronizers;";

			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				long id = resultSet.getLong("id");
				String sourceTableId = resultSet.getString("source_table_id");
				String targetTableId = resultSet.getString("target_table_id");
				String triggerName = resultSet.getString("trigger_name");
				String functionName = resultSet.getString("function_name");

				Map<ColumnRef, ColumnRef> mappingsForSynchronizer = syncColumMappings.get(id).stream()
						.map(columnMapping::get)
						.collect(Collectors.toMap(entry -> {
							TableColumn source = entry.getSource();
							if (!source.getTable().getTableId().equals(sourceTableId)) {
								throw new IllegalStateException("The column mapping of synchronizer: " + id
										+ " originates to a table: " + source.getTable().getTableId()
										+ " other than the source table: " + sourceTableId);
							}
							TableRef ref = refLog.getTableRefById(source.getTable().getTableId());
							return ref.getColumns().get(source.getColumn());
						}, entry -> {
							TableColumn target = entry.getTarget();
							if (!target.getTable().getTableId().equals(targetTableId)) {
								throw new IllegalStateException("The column mapping of synchronizer: " + id
										+ " targets into a table: " + target.getTable().getTableId()
										+ " other than the target table: " + targetTableId);
							}
							TableRef ref = refLog.getTableRefById(target.getTable().getTableId());
							return ref.getColumns().get(target.getColumn());
						}));

				refLog.addSync(triggerName, functionName, mappingsForSynchronizer);
			}
		}
	}

}
