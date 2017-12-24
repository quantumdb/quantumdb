package io.quantumdb.core.versioning;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import com.google.common.primitives.Ints;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import io.quantumdb.core.schema.definitions.PostgresTypes;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.ColumnType;
import io.quantumdb.core.schema.operations.Operation;
import io.quantumdb.core.utils.RandomHasher;
import io.quantumdb.core.versioning.RefLog.ColumnRef;
import io.quantumdb.core.versioning.RefLog.SyncRef;
import io.quantumdb.core.versioning.RefLog.TableRef;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Backend {

	@Data
	private static class TableId {
		private final String tableId;
	}

	@Data
	private static class TableColumn {
		private final long id;
		private final TableId tableId;
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
		private final String operationType;
		private final String operation;
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
		this.gson = new GsonBuilder()
				.registerTypeAdapter(ColumnType.class, (JsonDeserializer<ColumnType>) (element, type, context) -> {
					String fullType = element.getAsString().toUpperCase();

					String sqlType = fullType;
					if (fullType.contains("(")) {
						sqlType = fullType.substring(0, fullType.indexOf('('));
					}

					if (fullType.contains("(")) {
						int beginIndex = fullType.indexOf("(") + 1;
						int endIndex = fullType.lastIndexOf(")");
						List<Integer> arguments = Arrays.stream(fullType.substring(beginIndex, endIndex)
								.split(","))
								.map(String::trim)
								.map(Ints::tryParse)
								.collect(Collectors.toList());

						return PostgresTypes.from(sqlType, arguments.get(0));
					}
					return PostgresTypes.from(sqlType, null);
				})
				.registerTypeAdapter(ColumnType.class, (JsonSerializer<ColumnType>)
						(element, type, context) -> new JsonPrimitive(element.getNotation()))
				.create();
	}

	public State load(Connection connection, Catalog catalog) throws SQLException {
		Changelog changelog = loadChangelog(connection);
		Map<String, TableId> tableIds = listTableIds(connection);
		Table<TableId, Version, String> tableVersions = listTableVersions(connection, tableIds, changelog);
		List<TableColumn> tableColumns = listTableColumns(connection, tableIds);
		List<TableColumnMapping> columnMappings = listTableColumnMappings(connection, tableColumns);

		Multimap<TableId, TableColumn> columnsPerTable = LinkedHashMultimap.create();
		tableColumns.forEach(column -> columnsPerTable.put(column.getTableId(), column));

		Map<TableColumn, ColumnRef> columnCache = Maps.newLinkedHashMap();

		RefLog refLog = new RefLog();
		List<Version> toDo = Lists.newLinkedList();
		toDo.add(changelog.getRoot());

		while (!toDo.isEmpty()) {
			Version version = toDo.remove(0);
			for (Entry<TableId, String> entry : tableVersions.column(version).entrySet()) {
				TableRef tableRef = refLog.getTableRefs(version).stream()
						.filter(ref -> ref.getName().equals(entry.getValue()) && ref.getTableId().equals(entry.getKey().getTableId()))
						.findFirst()
						.orElse(null);

				if (tableRef != null) {
					tableRef.markAsPresent(version);
				}
				else {
					Map<TableColumn, ColumnRef> columnRefs = columnsPerTable.get(entry.getKey()).stream()
							.collect(Collectors.toMap(Function.identity(), column -> {
								List<ColumnRef> basedOn = columnMappings.stream()
										.filter(mapping -> mapping.getTarget().equals(column))
										.map(TableColumnMapping::getSource)
										.map(columnCache::get)
										.filter(ref -> ref != null)
										.collect(Collectors.toList());

								return new ColumnRef(column.getColumn(), basedOn);
							}, (l, r) -> l, LinkedHashMap::new));

					columnCache.putAll(columnRefs);
					refLog.addTable(entry.getValue(), entry.getKey().getTableId(), version, columnRefs.values());
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
		persistSynchronizerColumns(connection, syncRefs, columnMapping);
	}

	private void persistChangelog(Connection connection, Changelog changelog) throws SQLException {
		persistChangelogEntries(connection, changelog);
		persistChangesets(connection, changelog);
	}

	private void persistChangelogEntries(Connection connection, Changelog changelog) throws SQLException {
		Map<String, Version> mapping = Maps.newLinkedHashMap();
		List<Version> versions = Lists.newLinkedList();
		versions.add(changelog.getRoot());

		while (!versions.isEmpty()) {
			Version version = versions.remove(0);
			mapping.put(version.getId(), version);
			if (version.getChild() != null) {
				versions.add(version.getChild());
			}
		}

		Operations operations = new Operations();

		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb_changelog;";
			String deleteQuery = "DELETE FROM quantumdb_changelog WHERE version_id = ?;";
			String insertQuery = "INSERT INTO quantumdb_changelog (version_id, operation_type, operation, parent_version_id) VALUES (?, ?, ?, ?);";
			String updateQuery = "UPDATE quantumdb_changelog SET operation_type = ?, operation = ?, parent_version_id = ? WHERE version_id = ?;";

			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				String versionId = resultSet.getString("version_id");
				Version version = mapping.remove(versionId);
				if (version != null) {
					try (PreparedStatement update = connection.prepareStatement(updateQuery)) {
						Operation operation = version.getOperation();

						if (operation != null) {
							update.setString(1, operations.getOperationType(operation.getClass()).orElseThrow(
									() -> new IllegalArgumentException("There's no such operation as: " + operation.getClass())));

							update.setString(2, gson.toJson(operation));
						}
						else {
							update.setString(1, null);
							update.setString(2, null);
						}

						if (version.getParent() == null) {
							update.setString(3, null);
						}
						else {
							update.setString(3, version.getParent().getId());
						}
						update.setString(4, versionId);

						update.execute();
						log.debug("Updated entry for changelog id: {}", versionId);
					}
				}
				else {
					try (PreparedStatement delete = connection.prepareStatement(deleteQuery)) {
						delete.setString(1, versionId);
						delete.execute();
						log.debug("Deleted entry for changelog id: {}", versionId);
					}
				}
			}

			if (!mapping.isEmpty()) {
				PreparedStatement insert = connection.prepareStatement(insertQuery);
				for (Entry<String, Version> entry : mapping.entrySet()) {
					Version version = entry.getValue();
					Operation operation = version.getOperation();

					insert.setString(1, entry.getKey());

					if (operation != null) {
						insert.setString(2, operations.getOperationType(operation.getClass()).orElseThrow(
								() -> new IllegalArgumentException("There's no such operation as: " + operation.getClass())));

						insert.setString(3, gson.toJson(operation));
					}
					else {
						insert.setString(2, null);
						insert.setString(3, null);
					}

					String versionId = null;
					if (version.getParent() != null) {
						versionId = version.getParent().getId();
					}

					insert.setString(4, versionId);
					insert.execute();
					log.debug("Inserted new entry for changelog id: {}", versionId);
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
						log.debug("Updated entry for changeset id: {}", versionId);
					}
				}
				else {
					try (PreparedStatement delete = connection.prepareStatement(deleteQuery)) {
						delete.setString(1, versionId);
						delete.execute();
						log.debug("Deleted entry for changeset id: {}", versionId);
					}
				}
			}

			if (!mapping.isEmpty()) {
				PreparedStatement insert = connection.prepareStatement(insertQuery);
				for (Entry<String, ChangeSet> entry : mapping.entrySet()) {
					String versionId = entry.getKey();
					insert.setString(1, versionId);

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
					log.debug("Inserted new entry for changeset id: {}", versionId);
				}
			}

			resultSet.close();
		}
	}

	private void persistTables(Connection connection, RefLog refLog) throws SQLException {
		Set<String> tableIds = refLog.getTableRefs().stream()
				.map(TableRef::getTableId)
				.collect(Collectors.toSet());

		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb_tables ORDER BY table_id ASC;";
			String deleteQuery = "DELETE FROM quantumdb_tables WHERE table_id = ?;";
			String insertQuery = "INSERT INTO quantumdb_tables (table_id) VALUES (?);";

			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				String tableId = resultSet.getString("table_id");
				if (!tableIds.remove(tableId)) {
					try (Statement stmt = connection.createStatement()) {
						ResultSet r = stmt.executeQuery("SELECT * FROM quantumdb_column_mappings");
						printResults(r);
					}

					try (Statement stmt = connection.createStatement()) {
						ResultSet r = stmt.executeQuery("SELECT * FROM quantumdb_table_columns");
						printResults(r);
					}

					try (Statement stmt = connection.createStatement()) {
						ResultSet r = stmt.executeQuery("SELECT * FROM quantumdb_tables");
						printResults(r);
					}

					try (PreparedStatement delete = connection.prepareStatement(deleteQuery)) {
						delete.setString(1, tableId);
						delete.execute();
						log.debug("Deleted entry for table id: {}", tableId);
					}
				}
			}

			if (!tableIds.isEmpty()) {
				PreparedStatement insert = connection.prepareStatement(insertQuery);
				for (String tableId : tableIds) {
					insert.setString(1, tableId);
					insert.execute();
					log.debug("Inserted new entry for table id: {}", tableId);
				}
			}

			resultSet.close();
		}
	}

	private void printResults(ResultSet r) throws SQLException {
		ResultSetMetaData metaData = r.getMetaData();
		while (r.next()) {
			StringBuilder builder = new StringBuilder();
			builder.append("[" + metaData.getTableName(1) + "] ");
			for (int i = 0; i < metaData.getColumnCount(); i++) {
				builder.append(metaData.getColumnName(i + 1));
				builder.append("=");
				builder.append(r.getObject(i + 1));

				if (i < metaData.getColumnCount() - 1) {
					builder.append(", ");
				}
			}
			System.out.println(builder.toString());
		}
	}

	private void persistTableVersions(Connection connection, RefLog refLog) throws SQLException {
		Table<String, String, String> mapping = HashBasedTable.create();
		refLog.getTableRefs().forEach(tableRef -> {
			String tableId = tableRef.getTableId();
			String tableName = tableRef.getName();
			Set<String> versionIds = tableRef.getVersions().stream()
					.map(Version::getId)
					.collect(Collectors.toSet());

			versionIds.forEach(versionId -> mapping.put(tableId, versionId, tableName));
		});

		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb_table_versions ORDER BY table_id ASC;";
			String deleteQuery = "DELETE FROM quantumdb_table_versions WHERE table_id = ? AND version_id = ?;";
			String insertQuery = "INSERT INTO quantumdb_table_versions (table_id, version_id, table_name) VALUES (?, ?, ?);";

			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				String tableId = resultSet.getString("table_id");
				String versionId = resultSet.getString("version_id");

				if (mapping.containsRow(tableId)) {
					Map<String, String> internalMapping = mapping.row(tableId);
					if (!internalMapping.containsKey(versionId)) {
						try (PreparedStatement delete = connection.prepareStatement(deleteQuery)) {
							delete.setString(1, tableId);
							delete.setString(2, versionId);
							delete.execute();
							log.debug("Deleted entry for table_versions id: {} / {}", tableId, versionId);
						}
					}
					else {
						mapping.remove(tableId, versionId);
					}
				}
				else {
					try (PreparedStatement delete = connection.prepareStatement(deleteQuery)) {
						delete.setString(1, tableId);
						delete.setString(2, versionId);
						delete.execute();
						log.debug("Deleted entry for table_versions id: {} / {}", tableId, versionId);
					}
				}
			}

			if (!mapping.isEmpty()) {
				PreparedStatement insert = connection.prepareStatement(insertQuery);
				for (Cell<String, String, String> entry : mapping.cellSet()) {
					String tableId = entry.getRowKey();
					String versionId = entry.getColumnKey();

					insert.setString(1, tableId);
					insert.setString(2, versionId);
					insert.setString(3, entry.getValue());
					insert.execute();

					log.debug("Inserted new entry for table_versions id: {} / {}", tableId, versionId);
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
							log.debug("Deleted entry for table_columns id: {} - {} / {}", id, tableId, columnName);
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
						log.debug("Deleted entry for table_columns id: {}", id);
					}
				}
			}

			if (!columnMapping.isEmpty()) {
				try (PreparedStatement insert = connection.prepareStatement(insertQuery)) {
					for (Entry<String, String> entry : columnMapping.entries()) {
						String tableId = entry.getKey();
						String columnName = entry.getValue();

						insert.setString(1, tableId);
						insert.setString(2, columnName);
						ResultSet generatedKeys = insert.executeQuery();
						generatedKeys.next();

						long id = generatedKeys.getLong("id");
						columns.add(new RawTableColumn(id, tableId, columnName));
						log.debug("Inserted new entry for table_columns id: {} - {} / {}", id, tableId, columnName);
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
					columnMapping.put(target, source);
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
						log.debug("Deleted entry for column_mappings id: {}", id);
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
						log.debug("Inserted new entry for column_mappings id: {} - {} / {}", id, sourceId, targetId);
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
						log.debug("Updated entry for synchronizers id: {} - {} -> {}", id, sourceTableId, targetTableId);
					}

					mapping.put(id, syncRef);
				}
				else {
					try (PreparedStatement delete = connection.prepareStatement(deleteQuery)) {
						delete.setLong(1, id);
						delete.execute();
						log.debug("Deleted entry for synchronizers id: {}", id);
					}
				}
			}

			if (!syncMapping.isEmpty()) {
				PreparedStatement insert = connection.prepareStatement(insertQuery);
				for (SyncRef syncRef : syncMapping.values()) {
					String sourceTableId = syncRef.getSource().getTableId();
					String targetTableId = syncRef.getTarget().getTableId();

					insert.setString(1, sourceTableId);
					insert.setString(2, targetTableId);
					insert.setString(3, syncRef.getName());
					insert.setString(4, syncRef.getFunctionName());
					ResultSet generatedKeys = insert.executeQuery();

					generatedKeys.next();
					long id = generatedKeys.getLong("id");
					mapping.put(id, syncRef);
					log.debug("Inserted new entry for synchronizers id: {} - {} -> {}", id, sourceTableId, targetTableId);
				}
			}

			resultSet.close();
		}
		return mapping;
	}

	private void persistSynchronizerColumns(Connection connection, Map<Long, SyncRef> syncRefs,
			Map<Long, RawColumnMapping> columnMapping) throws SQLException {

		Table<String, String, Long> syncIndex = HashBasedTable.create();
		syncRefs.forEach((id, ref) -> syncIndex.put(ref.getSource().getTableId(), ref.getTarget().getTableId(), id));

		Multimap<Long, Long> idMapping = HashMultimap.create();
		columnMapping.forEach((id, mapping) -> {
			String sourceTableId = mapping.getSource().getTable();
			String targetTableId = mapping.getTarget().getTable();
			Long syncId = syncIndex.get(sourceTableId, targetTableId);
			if (syncId != null) {
				idMapping.put(syncId, mapping.getId());
			}
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
						log.debug("Deleted entry for synchronizer_columns id: {}, mapping: {}", synchronizerId, columnMappingId);
					}
				}
			}

			if (!idMapping.isEmpty()) {
				PreparedStatement insert = connection.prepareStatement(insertQuery);
				for (Entry<Long, Long> idEntry : idMapping.entries()) {
					Long synchronizerId = idEntry.getKey();
					Long columnMappingId = idEntry.getValue();
					insert.setLong(1, synchronizerId);
					insert.setLong(2, columnMappingId);
					insert.execute();
					log.debug("Inserted new entry for synchronizer_columns id: {}, mapping: {}", synchronizerId, columnMappingId);
				}
			}

			resultSet.close();
		}
	}

	private Changelog loadChangelog(Connection connection) throws SQLException {
		List<RawChangelogEntry> entries = loadChangelogEntries(connection);
		Map<String, RawChangeSet> changeSets = loadChangesets(connection, entries);

		Changelog changelog = null;
		List<RawChangelogEntry> changeSetContents = Lists.newArrayList();
		Operations operations = new Operations();

		while (!entries.isEmpty()) {
			RawChangelogEntry entry = entries.remove(0);
			changeSetContents.add(entry);
			boolean finalizeChangeSet = changeSets.containsKey(entry.getVersionId());

			if (entries.isEmpty()) {
				finalizeChangeSet = true;
			}

			if (finalizeChangeSet) {
				RawChangelogEntry first = changeSetContents.get(0);
				RawChangeSet rawChangeSet = changeSets.get(first.getVersionId());
				ChangeSet changeSet = new ChangeSet(rawChangeSet.getAuthor(), rawChangeSet.getCreated(), rawChangeSet.getDescription());

				for (RawChangelogEntry entryInSet : changeSetContents) {
					String operationType = entryInSet.getOperationType();
					Operation operation = null;
					if (operationType != null) {
						Class<? extends Operation> operationClass = operations.getOperationType(operationType)
								.orElseThrow(() -> new IllegalArgumentException("No such operation is supported: " + operationType));

						operation = gson.fromJson(entryInSet.getOperation(), operationClass);
					}

					if (changelog == null) {
						changelog = new Changelog(entryInSet.getVersionId(), changeSet);
					}
					else {
						String parentVersionId = entryInSet.getParentVersionId();
						Version parentVersion = changelog.getVersion(parentVersionId);
						changelog.addChangeSet(parentVersion, entryInSet.getVersionId(), changeSet, operation);
					}
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
				String operationType = resultSet.getString("operation_type");
				String operation = resultSet.getString("operation");
				String parentVersionId = resultSet.getString("parent_version_id");

				RawChangelogEntry entry = new RawChangelogEntry(versionId, operationType, operation, parentVersionId);
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
			root = new RawChangelogEntry(RandomHasher.generateHash(), null, null, null);
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

	private Map<String, RawChangeSet> loadChangesets(Connection connection, List<RawChangelogEntry> entries)
			throws SQLException {

		Map<String, RawChangeSet> changeSets = Maps.newHashMap();
		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb_changesets;";
			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				String versionId = resultSet.getString("version_id");
				String description = resultSet.getString("description");
				String author = resultSet.getString("author");
				Date created = resultSet.getTimestamp("created");

				RawChangeSet changeSet = new RawChangeSet(versionId, description, author, created);
				changeSets.put(versionId, changeSet);

			}
			resultSet.close();
		}

		if (changeSets.isEmpty()) {
			RawChangelogEntry rootEntry = entries.get(0);
			String versionId = rootEntry.getVersionId();
			RawChangeSet changeSet = new RawChangeSet(versionId, "Initial state of the database.", "QuantumDB", new Date());
			changeSets.put(versionId, changeSet);
		}

		return changeSets;
	}

	private Map<String, TableId> listTableIds(Connection connection) throws SQLException {
		Map<String, TableId> tableIds = Maps.newHashMap();
		try (Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery("SELECT * FROM quantumdb_tables ORDER BY table_id ASC;");
			while (resultSet.next()) {
				String tableId = resultSet.getString("table_id");
				tableIds.put(tableId, new TableId(tableId));
			}
		}
		return tableIds;
	}

	private Table<TableId, Version, String> listTableVersions(Connection connection, Map<String, TableId> tableIds, Changelog changelog) throws SQLException {
		Table<TableId, Version, String> mapping = HashBasedTable.create();
		try (Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery("SELECT * FROM quantumdb_table_versions ORDER BY table_id ASC;");
			while (resultSet.next()) {
				String tableId = resultSet.getString("table_id");
				String tableName = resultSet.getString("table_name");
				String versionId = resultSet.getString("version_id");
				Version version = changelog.getVersion(versionId);
				TableId tableIdRef = tableIds.get(tableId);
				mapping.put(tableIdRef, version, tableName);
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
				TableId tableIdRef = tableIds.get(tableId);
				results.add(new TableColumn(id, tableIdRef, column));
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

		Multimap<Long, Long> syncColumnMappings = HashMultimap.create();
		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb_synchronizer_columns;";

			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				long synchronizerId = resultSet.getLong("synchronizer_id");
				long columnMappingId = resultSet.getLong("column_mapping_id");
				syncColumnMappings.put(synchronizerId, columnMappingId);
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

				Map<ColumnRef, ColumnRef> mappingsForSynchronizer = syncColumnMappings.get(id).stream()
						.map(columnMapping::get)
						.collect(Collectors.toMap(entry -> {
							TableColumn source = entry.getSource();
							if (!source.getTableId().getTableId().equals(sourceTableId)) {
								throw new IllegalStateException("The column mapping of synchronizer: " + id
										+ " originates to a table: " + source.getTableId().getTableId()
										+ " other than the source table: " + sourceTableId);
							}
							TableRef ref = refLog.getTableRefById(source.getTableId().getTableId());
							return ref.getColumns().get(source.getColumn());
						}, entry -> {
							TableColumn target = entry.getTarget();
							if (!target.getTableId().getTableId().equals(targetTableId)) {
								throw new IllegalStateException("The column mapping of synchronizer: " + id
										+ " targets into a table: " + target.getTableId().getTableId()
										+ " other than the target table: " + targetTableId);
							}
							TableRef ref = refLog.getTableRefById(target.getTableId().getTableId());
							return ref.getColumns().get(target.getColumn());
						}));

				if (!mappingsForSynchronizer.isEmpty()) {
					refLog.addSync(triggerName, functionName, mappingsForSynchronizer);
				}
			}
		}
	}

}
