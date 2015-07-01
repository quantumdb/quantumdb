package io.quantumdb.core.versioning;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Map;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.versioning.ChangeSetDataBackend.ChangeSetEntry;

class ChangeSetDataBackend implements PrimaryKeyBackend<String, ChangeSetEntry> {

	static class ChangeSetEntry {

		private final ResultSet resultSet;
		private int row;
		private boolean isNew;
		private boolean isChanged;
		private boolean isDeleted;

		private String versionId;
		private String author;
		private Date created;
		private String description;

		ChangeSetEntry(ResultSet resultSet, boolean isNew) throws SQLException {
			this.resultSet = resultSet;
			this.row = isNew ? -1 : resultSet.getRow();
			this.isNew = isNew;
			this.isChanged = false;
			this.isDeleted = false;

			if (!isNew) {
				this.versionId = resultSet.getString("version_id");
				this.author = resultSet.getString("author");
				this.created = resultSet.getDate("created");
				this.description = resultSet.getString("description");
			}
		}

		public String getVersionId() {
			return versionId;
		}

		public String getAuthor() {
			return author;
		}

		public void setAuthor(String author) {
			this.author = author;
			this.isChanged = true;
		}

		public java.util.Date getCreated() {
			return created;
		}

		public void setCreated(java.util.Date created) {
			this.created = new Date(created.getTime());
			this.isChanged = true;
		}

		public String getDescription() {
			return description;
		}

		public void setDescription(String description) {
			this.description = description;
			this.isChanged = true;
		}

		void delete() {
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

			resultSet.updateString("version_id", versionId);
			resultSet.updateString("author", author);
			resultSet.updateDate("created", created);
			resultSet.updateString("description", description);

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

	private final Map<String, ChangeSetEntry> entries = Maps.newLinkedHashMap();
	private ResultSet pointer;

	@Override
	public Map<String, ChangeSetEntry> load(Backend backend, Connection connection) throws SQLException {
		entries.clear();

		pointer = query(connection, "quantumdb_changesets", "created");
		while (pointer.next()) {
			ChangeSetEntry entry = new ChangeSetEntry(pointer, false);
			entries.put(entry.getVersionId(), entry);
		}

		return Collections.unmodifiableMap(entries);
	}

	@Override
	public ChangeSetEntry create(String key) throws SQLException {
		ChangeSetEntry entry = new ChangeSetEntry(pointer, true);
		entry.versionId = key;
		entries.put(key, entry);
		return entry;
	}

	@Override
	public void delete(String key) throws SQLException {
		ChangeSetEntry entry = entries.remove(key);
		if (entry != null) {
			entry.delete();
		}
	}

	@Override
	public void persist() throws SQLException {
		for (ChangeSetEntry entry : entries.values()) {
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
