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
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import com.google.common.primitives.Ints;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.ColumnType;
import io.quantumdb.core.schema.definitions.PostgresTypes;
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
	private static class RefId {
		private final String refId;
	}

	@Data
	private static class TableColumn {
		private final long id;
		private final RefId refId;
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
		private final String id;
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
		Map<String, RefId> refIds = listRefIds(connection);
		Table<RefId, Version, String> tableVersions = listTableVersions(connection, refIds, changelog);
		List<TableColumn> tableColumns = listTableColumns(connection, refIds);
		List<TableColumnMapping> columnMappings = listTableColumnMappings(connection, tableColumns);

		Multimap<RefId, TableColumn> columnsPerTable = LinkedHashMultimap.create();
		tableColumns.forEach(column -> columnsPerTable.put(column.getRefId(), column));

		Map<TableColumn, ColumnRef> columnCache = Maps.newLinkedHashMap();

		RefLog refLog = new RefLog();
		List<Version> toDo = Lists.newLinkedList();
		toDo.add(changelog.getRoot());

		while (!toDo.isEmpty()) {
			Version version = toDo.remove(0);
			for (Entry<RefId, String> entry : tableVersions.column(version).entrySet()) {
				TableRef tableRef = refLog.getTableRefs(version).stream()
						.filter(ref -> ref.getName().equals(entry.getValue()) && ref.getRefId().equals(entry.getKey().getRefId()))
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
					refLog.addTable(entry.getValue(), entry.getKey().getRefId(), version, columnRefs.values());
				}
			}

			if (version.getChild() != null) {
				toDo.add(version.getChild());
			}
		}

		addSynchronizers(connection, refLog, columnMappings);
		setActiveVersions(connection, changelog, refLog);

		return new State(catalog, refLog, changelog);
	}

	public void persist(Connection connection, State state) throws SQLException {
		persistChangelog(connection, state.getChangelog());

		RefLog refLog = state.getRefLog();
		persistRefs(connection, refLog);
		persistRefVersions(connection, refLog);
		Collection<RawTableColumn> columns = persistTableColumns(connection, refLog);
		Map<Long, RawColumnMapping> columnMapping = persistColumnMappings(connection, refLog, columns);
		Map<Long, SyncRef> syncRefs = persistTableSynchronizers(connection, refLog);
		persistSynchronizerColumns(connection, syncRefs, columnMapping);
		persistActiveVersions(connection, refLog);
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
			String query = "SELECT * FROM quantumdb.changelog;";
			String deleteQuery = "DELETE FROM quantumdb.changelog WHERE version_id = ?;";
			String insertQuery = "INSERT INTO quantumdb.changelog (version_id, operation_type, operation, parent_version_id) VALUES (?, ?, ?, ?);";
			String updateQuery = "UPDATE quantumdb.changelog SET operation_type = ?, operation = ?, parent_version_id = ? WHERE version_id = ?;";

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
			ChangeSet changeSet = version.getChangeSet();
			if (mapping.containsValue(changeSet)) {
				List<String> versionIdsToDrop = mapping.entrySet().stream()
						.filter(entry -> entry.getValue().equals(changeSet))
						.map(Entry::getKey)
						.collect(Collectors.toList());

				versionIdsToDrop.forEach(mapping::remove);
			}
			mapping.put(version.getId(), changeSet);
			if (version.getChild() != null) {
				versions.add(version.getChild());
			}
		}

		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb.changesets;";
			String deleteQuery = "DELETE FROM quantumdb.changesets WHERE version_id = ?;";
			String insertQuery = "INSERT INTO quantumdb.changesets (id, version_id, author, description, created) VALUES (?, ?, ?, ?, ?);";
			String updateQuery = "UPDATE quantumdb.changesets SET author = ?, description = ?, created = ? WHERE version_id = ?;";

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
					ChangeSet changeSet = entry.getValue();

					if (changeSet != null) {
						insert.setString(1, changeSet.getId());
						insert.setString(2, versionId);
						insert.setString(3, changeSet.getAuthor());
						insert.setString(4, changeSet.getDescription());
						insert.setTimestamp(5, new Timestamp(changeSet.getCreated().getTime()));
					}
					else {
						insert.setString(1, versionId);
						insert.setString(2, "");
						insert.setString(3, "");
						insert.setTimestamp(4, Timestamp.from(Instant.now()));
						throw new IllegalArgumentException("This would be weird!");
					}
					insert.execute();
					log.debug("Inserted new entry for changeset id: {}", versionId);
				}
			}

			resultSet.close();
		}
	}

	private void persistRefs(Connection connection, RefLog refLog) throws SQLException {
		Set<String> refIds = Sets.newHashSet();
		refLog.getTableRefs().forEach(ref -> refIds.add(ref.getRefId()));
		refLog.getViewRefs().forEach(ref -> refIds.add(ref.getRefId()));

		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb.refs ORDER BY ref_id ASC;";
			String deleteQuery = "DELETE FROM quantumdb.refs WHERE ref_id = ?;";
			String insertQuery = "INSERT INTO quantumdb.refs (ref_id) VALUES (?);";

			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				String refId = resultSet.getString("ref_id");
				if (!refIds.remove(refId)) {
					try (Statement stmt = connection.createStatement()) {
						ResultSet r = stmt.executeQuery("SELECT * FROM quantumdb.column_mappings");
						printResults(r);
					}

					try (Statement stmt = connection.createStatement()) {
						ResultSet r = stmt.executeQuery("SELECT * FROM quantumdb.table_columns");
						printResults(r);
					}

					try (Statement stmt = connection.createStatement()) {
						ResultSet r = stmt.executeQuery("SELECT * FROM quantumdb.refs");
						printResults(r);
					}

					try (PreparedStatement delete = connection.prepareStatement(deleteQuery)) {
						delete.setString(1, refId);
						delete.execute();
						log.debug("Deleted entry for ref id: {}", refId);
					}
				}
			}

			if (!refIds.isEmpty()) {
				PreparedStatement insert = connection.prepareStatement(insertQuery);
				for (String refId : refIds) {
					insert.setString(1, refId);
					insert.execute();
					log.debug("Inserted new entry for ref id: {}", refId);
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

	private void persistRefVersions(Connection connection, RefLog refLog) throws SQLException {
		Table<String, String, String> mapping = HashBasedTable.create();
		refLog.getTableRefs().forEach(tableRef -> {
			String refId = tableRef.getRefId();
			String tableName = tableRef.getName();
			Set<String> versionIds = tableRef.getVersions().stream()
					.map(Version::getId)
					.collect(Collectors.toSet());

			versionIds.forEach(versionId -> mapping.put(refId, versionId, tableName));
		});

		refLog.getViewRefs().forEach(viewRef -> {
			String refId = viewRef.getRefId();
			String viewName = viewRef.getName();
			Set<String> versionIds = viewRef.getVersions().stream()
					.map(Version::getId)
					.collect(Collectors.toSet());

			versionIds.forEach(versionId -> mapping.put(refId, versionId, viewName));
		});

		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb.ref_versions ORDER BY ref_id ASC;";
			String deleteQuery = "DELETE FROM quantumdb.ref_versions WHERE ref_id = ? AND version_id = ?;";
			String insertQuery = "INSERT INTO quantumdb.ref_versions (ref_id, version_id, table_name) VALUES (?, ?, ?);";

			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				String refId = resultSet.getString("ref_id");
				String versionId = resultSet.getString("version_id");

				if (mapping.containsRow(refId)) {
					Map<String, String> internalMapping = mapping.row(refId);
					if (!internalMapping.containsKey(versionId)) {
						try (PreparedStatement delete = connection.prepareStatement(deleteQuery)) {
							delete.setString(1, refId);
							delete.setString(2, versionId);
							delete.execute();
							log.debug("Deleted entry for ref_versions id: {} / {}", refId, versionId);
						}
					}
					else {
						mapping.remove(refId, versionId);
					}
				}
				else {
					try (PreparedStatement delete = connection.prepareStatement(deleteQuery)) {
						delete.setString(1, refId);
						delete.setString(2, versionId);
						delete.execute();
						log.debug("Deleted entry for ref_versions id: {} / {}", refId, versionId);
					}
				}
			}

			if (!mapping.isEmpty()) {
				PreparedStatement insert = connection.prepareStatement(insertQuery);
				for (Cell<String, String, String> entry : mapping.cellSet()) {
					String refId = entry.getRowKey();
					String versionId = entry.getColumnKey();

					insert.setString(1, refId);
					insert.setString(2, versionId);
					insert.setString(3, entry.getValue());
					insert.execute();

					log.debug("Inserted new entry for ref_versions id: {} / {}", refId, versionId);
				}
			}

			resultSet.close();
		}
	}

	private Collection<RawTableColumn> persistTableColumns(Connection connection, RefLog refLog) throws SQLException {
		Multimap<String, String> columnMapping = LinkedHashMultimap.create();
		refLog.getTableRefs()
				.forEach(tableRef -> columnMapping.putAll(tableRef.getRefId(), tableRef.getColumns().keySet()));

		List<RawTableColumn> columns = Lists.newArrayList();
		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb.table_columns ORDER BY id ASC;";
			String deleteQuery = "DELETE FROM quantumdb.table_columns WHERE id = ?;";
			String insertQuery = "INSERT INTO quantumdb.table_columns (ref_id, column_name) VALUES (?, ?) RETURNING id;";

			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				Long id = resultSet.getLong("id");
				String refId = resultSet.getString("ref_id");
				if (columnMapping.containsKey(refId)) {
					Collection<String> columnNames = columnMapping.get(refId);
					String columnName = resultSet.getString("column_name");
					if (!columnNames.contains(columnName)) {
						try (PreparedStatement delete = connection.prepareStatement(deleteQuery)) {
							delete.setLong(1, id);
							delete.execute();
							log.debug("Deleted entry for table_columns id: {} - {} / {}", id, refId, columnName);
						}
					}
					else {
						columnMapping.remove(refId, columnName);
						columns.add(new RawTableColumn(id, refId, columnName));
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
						String refId = entry.getKey();
						String columnName = entry.getValue();

						insert.setString(1, refId);
						insert.setString(2, columnName);
						ResultSet generatedKeys = insert.executeQuery();
						generatedKeys.next();

						long id = generatedKeys.getLong("id");
						columns.add(new RawTableColumn(id, refId, columnName));
						log.debug("Inserted new entry for table_columns id: {} - {} / {}", id, refId, columnName);
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
			String refId = tableRef.getRefId();
			for (ColumnRef columnRef : tableRef.getColumns().values()) {
				String columnName = columnRef.getName();
				RawColumn target = new RawColumn(refId, columnName);

				for (ColumnRef basedOn : columnRef.getBasedOn()) {
					String sourceRefId = basedOn.getTable().getRefId();
					String sourceColumnName = basedOn.getName();
					RawColumn source = new RawColumn(sourceRefId, sourceColumnName);

					columnMapping.put(source, target);
					columnMapping.put(target, source);
				}
			}
		}

		Map<Long, RawColumnMapping> results = Maps.newHashMap();
		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb.column_mappings;";
			String deleteQuery = "DELETE FROM quantumdb.column_mappings WHERE id = ?;";
			String insertQuery = "INSERT INTO quantumdb.column_mappings (source_column_id, target_column_id) VALUES (?, ?) RETURNING id;";

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
				String sourceId = syncRef.getSource().getRefId();
				String targetId = syncRef.getTarget().getRefId();
				syncMapping.put(sourceId, targetId, syncRef);
			}
		}

		Map<Long, SyncRef> mapping = Maps.newHashMap();
		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb.synchronizers;";
			String deleteQuery = "DELETE FROM quantumdb.synchronizers WHERE id = ?;";
			String insertQuery = "INSERT INTO quantumdb.synchronizers (source_ref_id, target_ref_id, trigger_name, function_name) VALUES (?, ?, ?, ?) RETURNING id;";
			String updateQuery = "UPDATE quantumdb.synchronizers SET trigger_name = ?, function_name = ? WHERE id = ?;";

			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				long id = resultSet.getLong("id");
				String sourceRefId = resultSet.getString("source_ref_id");
				String targetRefId = resultSet.getString("target_ref_id");

				if (syncMapping.contains(sourceRefId, targetRefId)) {
					SyncRef syncRef = syncMapping.remove(sourceRefId, targetRefId);

					try (PreparedStatement update = connection.prepareStatement(updateQuery)) {
						update.setString(1, syncRef.getName());
						update.setString(2, syncRef.getFunctionName());
						update.setLong(3, id);
						update.execute();
						log.debug("Updated entry for synchronizers id: {} - {} -> {}", id, sourceRefId, targetRefId);
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
					String sourceRefId = syncRef.getSource().getRefId();
					String targetRefId = syncRef.getTarget().getRefId();

					insert.setString(1, sourceRefId);
					insert.setString(2, targetRefId);
					insert.setString(3, syncRef.getName());
					insert.setString(4, syncRef.getFunctionName());
					ResultSet generatedKeys = insert.executeQuery();

					generatedKeys.next();
					long id = generatedKeys.getLong("id");
					mapping.put(id, syncRef);
					log.debug("Inserted new entry for synchronizers id: {} - {} -> {}", id, sourceRefId, targetRefId);
				}
			}

			resultSet.close();
		}
		return mapping;
	}

	private void persistSynchronizerColumns(Connection connection, Map<Long, SyncRef> syncRefs,
			Map<Long, RawColumnMapping> columnMapping) throws SQLException {

		Table<String, String, Long> syncIndex = HashBasedTable.create();
		syncRefs.forEach((id, ref) -> syncIndex.put(ref.getSource().getRefId(), ref.getTarget().getRefId(), id));

		Multimap<Long, Long> idMapping = HashMultimap.create();
		columnMapping.forEach((id, mapping) -> {
			String sourceRefId = mapping.getSource().getTable();
			String targetRefId = mapping.getTarget().getTable();
			Long syncId = syncIndex.get(sourceRefId, targetRefId);
			if (syncId != null) {
				idMapping.put(syncId, mapping.getId());
			}
		});

		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb.synchronizer_columns;";
			String deleteQuery = "DELETE FROM quantumdb.synchronizer_columns WHERE synchronizer_id = ? AND column_mapping = ?;";
			String insertQuery = "INSERT INTO quantumdb.synchronizer_columns (synchronizer_id, column_mapping_id) VALUES (?, ?);";

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

	private void persistActiveVersions(Connection connection, RefLog refLog) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb.active_versions;";
			String deleteQuery = "DELETE FROM quantumdb.active_versions WHERE version_id = ?;";
			String insertQuery = "INSERT INTO quantumdb.active_versions (version_id) VALUES (?);";

			ResultSet resultSet = statement.executeQuery(query);
			Set<String> versions = refLog.getVersions().stream()
					.map(Version::getId)
					.collect(Collectors.toSet());

			while (resultSet.next()) {
				String versionId = resultSet.getString("version_id");
				if (versions.contains(versionId)) {
					versions.remove(versionId);
				}
				else {
					try (PreparedStatement delete = connection.prepareStatement(deleteQuery)) {
						delete.setString(1, versionId);
						delete.execute();
						log.debug("Deleted entry for active_versions id: {}", versionId);
					}
				}
			}

			if (!versions.isEmpty()) {
				PreparedStatement insert = connection.prepareStatement(insertQuery);
				for (String versionId : versions) {
					insert.setString(1, versionId);
					insert.execute();
					log.debug("Inserted new entry for active_versions id: {}", versionId);
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
				RawChangelogEntry lastEntry = changeSetContents.get(changeSetContents.size() - 1);
				RawChangeSet rawChangeSet = changeSets.get(lastEntry.getVersionId());
				ChangeSet changeSet = new ChangeSet(rawChangeSet.getId(), rawChangeSet.getAuthor(), rawChangeSet.getCreated(), rawChangeSet.getDescription());

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
		}

		return changelog;
	}

	private List<RawChangelogEntry> loadChangelogEntries(Connection connection) throws SQLException {
		RawChangelogEntry root = null;
		Multimap<String, RawChangelogEntry> entries = HashMultimap.create();
		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb.changelog;";
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
			String query = "SELECT * FROM quantumdb.changesets;";
			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				String id = resultSet.getString("id");
				String versionId = resultSet.getString("version_id");
				String description = resultSet.getString("description");
				String author = resultSet.getString("author");
				Date created = resultSet.getTimestamp("created");

				RawChangeSet changeSet = new RawChangeSet(id, versionId, description, author, created);
				changeSets.put(versionId, changeSet);

			}
			resultSet.close();
		}

		if (changeSets.isEmpty()) {
			RawChangelogEntry rootEntry = entries.get(0);
			String versionId = rootEntry.getVersionId();
			RawChangeSet changeSet = new RawChangeSet("initial", versionId, "Initial state of the database.", "QuantumDB", new Date());
			changeSets.put(versionId, changeSet);
		}

		return changeSets;
	}

	private Map<String, RefId> listRefIds(Connection connection) throws SQLException {
		Map<String, RefId> refIds = Maps.newHashMap();
		try (Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery("SELECT * FROM quantumdb.refs ORDER BY ref_id ASC;");
			while (resultSet.next()) {
				String refId = resultSet.getString("ref_id");
				refIds.put(refId, new RefId(refId));
			}
		}
		return refIds;
	}

	private Table<RefId, Version, String> listTableVersions(Connection connection, Map<String, RefId> refIds, Changelog changelog) throws SQLException {
		Table<RefId, Version, String> mapping = HashBasedTable.create();
		try (Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery("SELECT * FROM quantumdb.ref_versions ORDER BY ref_id ASC;");
			while (resultSet.next()) {
				String refId = resultSet.getString("ref_id");
				String tableName = resultSet.getString("table_name");
				String versionId = resultSet.getString("version_id");
				Version version = changelog.getVersion(versionId);
				RefId refIdRef = refIds.get(refId);
				mapping.put(refIdRef, version, tableName);
			}
		}
		return mapping;
	}

	private List<TableColumn> listTableColumns(Connection connection, Map<String, RefId> refIds) throws SQLException {
		List<TableColumn> results = Lists.newArrayList();
		try (Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery("SELECT * FROM quantumdb.table_columns ORDER BY id ASC;");
			while (resultSet.next()) {
				long id = resultSet.getLong("id");
				String refId = resultSet.getString("ref_id");
				String column = resultSet.getString("column_name");
				RefId refIdRef = refIds.get(refId);
				results.add(new TableColumn(id, refIdRef, column));
			}
		}
		return results;
	}

	private List<TableColumnMapping> listTableColumnMappings(Connection connection, List<TableColumn> tableColumns) throws SQLException {
		Map<Long, TableColumn> columnById = tableColumns.stream()
				.collect(Collectors.toMap(TableColumn::getId, Function.identity()));

		List<TableColumnMapping> results = Lists.newArrayList();
		try (Statement statement = connection.createStatement()) {
			ResultSet resultSet = statement.executeQuery("SELECT * FROM quantumdb.column_mappings;");
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
			String query = "SELECT * FROM quantumdb.synchronizer_columns;";

			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				long synchronizerId = resultSet.getLong("synchronizer_id");
				long columnMappingId = resultSet.getLong("column_mapping_id");
				syncColumnMappings.put(synchronizerId, columnMappingId);
			}
		}

		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb.synchronizers;";

			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				long id = resultSet.getLong("id");
				String sourceRefId = resultSet.getString("source_ref_id");
				String targetRefId = resultSet.getString("target_ref_id");
				String triggerName = resultSet.getString("trigger_name");
				String functionName = resultSet.getString("function_name");

				Map<ColumnRef, ColumnRef> mappingsForSynchronizer = syncColumnMappings.get(id).stream()
						.map(columnMapping::get)
						.collect(Collectors.toMap(entry -> {
							TableColumn source = entry.getSource();
							if (!source.getRefId().getRefId().equals(sourceRefId)) {
								throw new IllegalStateException("The column mapping of synchronizer: " + id
										+ " originates to a table: " + source.getRefId().getRefId()
										+ " other than the source table: " + sourceRefId);
							}
							TableRef ref = refLog.getTableRefById(source.getRefId().getRefId());
							return ref.getColumns().get(source.getColumn());
						}, entry -> {
							TableColumn target = entry.getTarget();
							if (!target.getRefId().getRefId().equals(targetRefId)) {
								throw new IllegalStateException("The column mapping of synchronizer: " + id
										+ " targets into a table: " + target.getRefId().getRefId()
										+ " other than the target table: " + targetRefId);
							}
							TableRef ref = refLog.getTableRefById(target.getRefId().getRefId());
							return ref.getColumns().get(target.getColumn());
						}));

				if (!mappingsForSynchronizer.isEmpty()) {
					refLog.addSync(triggerName, functionName, mappingsForSynchronizer);
				}
			}
		}
	}

	private void setActiveVersions(Connection connection, Changelog changelog, RefLog refLog) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			String query = "SELECT * FROM quantumdb.active_versions;";

			Set<String> activeVersions = Sets.newHashSet();
			ResultSet resultSet = statement.executeQuery(query);
			while (resultSet.next()) {
				String versionId = resultSet.getString("version_id");
				activeVersions.add(versionId);
			}

			Version pointer = changelog.getRoot();
			while (pointer != null) {
				String versionId = pointer.getId();
				if (activeVersions.contains(versionId)) {
					activeVersions.remove(versionId);
					Version version = changelog.getVersion(versionId);
					refLog.setVersionState(version, true);
				}
				pointer = pointer.getChild();
			}

			if (!activeVersions.isEmpty()) {
				throw new IllegalStateException("There's are active versions defined which are not present or " +
						"reachable in the changelog: " + activeVersions.stream().collect(Collectors.joining(",")));
			}
		}

	}

}
