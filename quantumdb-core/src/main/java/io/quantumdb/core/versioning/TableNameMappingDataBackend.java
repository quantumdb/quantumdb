package io.quantumdb.core.versioning;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.versioning.TableNameMappingDataBackend.TableMappingEntry;
import io.quantumdb.core.versioning.TableNameMappingDataBackend.TableMappingKey;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class TableNameMappingDataBackend implements DataBackend<TableMappingKey, TableMappingEntry> {

	@Data
	static class TableMappingKey {
		private final String tableName;
		private final String versionId;
	}

	static class TableMappingEntry {

		private final ResultSet resultSet;
		private int row;
		private boolean isNew;
		private boolean isChanged;
		private boolean isDeleted;

		private String tableName;
		private String tableId;
		private String versionId;

		TableMappingEntry(ResultSet resultSet, boolean isNew) throws SQLException {
			this.resultSet = resultSet;
			this.row = isNew ? 0 : resultSet.getRow();
			this.isNew = isNew;
			this.isChanged = false;
			this.isDeleted = false;

			if (!isNew) {
				this.tableName = resultSet.getString("table_name");
				this.tableId = resultSet.getString("table_id");
				this.versionId = resultSet.getString("version_id");
			}
		}

		public String getTableName() throws SQLException {
			return tableName;
		}

		public String getVersionId() throws SQLException {
			return versionId;
		}

		public String getTableId() throws SQLException {
			return tableId;
		}

		public void setTableId(String tableId) throws SQLException {
			this.tableId = tableId;
			this.isChanged = true;
		}

		private void delete() throws SQLException {
			this.isDeleted = true;
		}

		void persist() throws SQLException {
			if (!isNew) {
				resultSet.first();
				while (resultSet.getRow() != row) {
					resultSet.next();
				}
			}
			else {
				resultSet.moveToInsertRow();
			}

			resultSet.updateString("table_name", tableName);
			resultSet.updateString("table_id", tableId);
			resultSet.updateString("version_id", versionId);

			if (isDeleted) {
				resultSet.deleteRow();
				isChanged = false;
			}
			else if (isNew) {
				resultSet.insertRow();
				resultSet.last();
				row = resultSet.getRow();
				isNew = false;
				isChanged = false;
			}
			else if (isChanged) {
				resultSet.updateRow();
				isChanged = false;
			}
		}
	}

	private final Map<TableMappingKey, TableMappingEntry> entries = Maps.newLinkedHashMap();
	private ResultSet pointer;

	@Override
	public Map<TableMappingKey, TableMappingEntry> load(Backend backend, Connection connection) throws SQLException {
		entries.clear();

		pointer = query(connection, "quantumdb_tablemappings", null);
		while (pointer.next()) {
			TableMappingEntry entry = new TableMappingEntry(pointer, false);
			entries.put(new TableMappingKey(entry.getTableName(), entry.getVersionId()), entry);
		}

		return Maps.filterEntries(entries, entry -> !entry.getValue().isDeleted);
	}

	@Override
	public TableMappingEntry create(TableMappingKey key) throws SQLException {
		TableMappingEntry entry = new TableMappingEntry(pointer, true);
		entry.tableName = key.getTableName();
		entry.versionId = key.getVersionId();
		entries.put(key, entry);
		return entry;
	}

	@Override
	public void delete(TableMappingKey key) throws SQLException {
		TableMappingEntry entry = entries.remove(key);
		if (entry != null) {
			entry.delete();
		}
	}

	@Override
	public void persist() throws SQLException {
		for (TableMappingEntry entry : entries.values()) {
			entry.persist();
		}
	}

	private ResultSet query(Connection connection, String tableName, String orderBy) throws SQLException {
		Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
		String query = "SELECT * FROM " + tableName;
		if (!Strings.isNullOrEmpty(orderBy)) {
			query += " ORDER BY " + orderBy;
		}

		return  statement.executeQuery(query);
	}

}
