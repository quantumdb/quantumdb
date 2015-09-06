package io.quantumdb.core.versioning;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Map;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.postgresql.PostgresTypes;
import io.quantumdb.core.schema.definitions.ColumnType;
import io.quantumdb.core.schema.operations.AddColumn;
import io.quantumdb.core.schema.operations.AddForeignKey;
import io.quantumdb.core.schema.operations.AlterColumn;
import io.quantumdb.core.schema.operations.CopyTable;
import io.quantumdb.core.schema.operations.CreateIndex;
import io.quantumdb.core.schema.operations.CreateTable;
import io.quantumdb.core.schema.operations.DecomposeTable;
import io.quantumdb.core.schema.operations.DropColumn;
import io.quantumdb.core.schema.operations.DropForeignKey;
import io.quantumdb.core.schema.operations.DropIndex;
import io.quantumdb.core.schema.operations.DropTable;
import io.quantumdb.core.schema.operations.JoinTable;
import io.quantumdb.core.schema.operations.MergeTable;
import io.quantumdb.core.schema.operations.PartitionTable;
import io.quantumdb.core.schema.operations.RenameTable;
import io.quantumdb.core.schema.operations.SchemaOperation;
import io.quantumdb.core.versioning.ChangeLogDataBackend.ChangeLogEntry;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ChangeLogDataBackend implements PrimaryKeyBackend<String, ChangeLogEntry> {

	static class ChangeLogEntry {

		private final ResultSet resultSet;
		private final Gson serizalizer;
		private int row;
		private boolean isNew;
		private boolean isChanged;
		private boolean isDeleted;

		private String versionId;
		private SchemaOperation schemaOperation;
		private String parentVersionId;

		ChangeLogEntry(ResultSet resultSet, Gson serializer, boolean isNew) throws SQLException {
			this.resultSet = resultSet;
			this.serizalizer = serializer;
			this.row = isNew ? -1 : resultSet.getRow();
			this.isNew = isNew;
			this.isChanged = false;
			this.isDeleted = false;

			if (!isNew) {
				this.versionId = resultSet.getString("version_id");
				this.schemaOperation = serializer.fromJson(resultSet.getString("schema_operation"), SchemaOperation.class);
				this.parentVersionId = resultSet.getString("parent_version_id");
			}
		}

		public String getVersionId() {
			return versionId;
		}

		public SchemaOperation getSchemaOperation() {
			return schemaOperation;
		}

		public void setSchemaOperation(SchemaOperation operation) {
			this.schemaOperation = operation;
			this.isChanged = true;
		}

		public String getParentVersionId() {
			return parentVersionId;
		}

		public void setParentVersionId(String parentVersionId) {
			this.parentVersionId = parentVersionId;
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
			resultSet.updateString("schema_operation", serizalizer.toJson(schemaOperation));
			resultSet.updateString("parent_version_id", parentVersionId);

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

	private final Gson serializer = createSerializer();
	private final Map<String, ChangeLogEntry> entries = Maps.newLinkedHashMap();
	private ResultSet pointer;

	@Override
	public Map<String, ChangeLogEntry> load(Backend backend, Connection connection) throws SQLException {
		entries.clear();

		pointer = query(connection, "quantumdb_changelog", null);
		while (pointer.next()) {
			ChangeLogEntry entry = new ChangeLogEntry(pointer, serializer, false);
			entries.put(entry.getVersionId(), entry);
		}

		return Collections.unmodifiableMap(entries);
	}

	@Override
	public ChangeLogEntry create(String key) throws SQLException {
		ChangeLogEntry entry = new ChangeLogEntry(pointer, serializer, true);
		entry.versionId = key;
		entries.put(key, entry);
		return entry;
	}

	@Override
	public void delete(String key) throws SQLException {
		ChangeLogEntry entry = entries.remove(key);
		if (entry != null) {
			entry.delete();
		}
	}

	@Override
	public void persist() throws SQLException {
		for (ChangeLogEntry entry : entries.values()) {
			entry.persist();
		}
	}

	private ResultSet query(Connection connection, String tableName, String orderBy) throws SQLException {
		Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
		String query = "SELECT * FROM " + tableName;
		if (!Strings.isNullOrEmpty(orderBy)) {
			query += " ORDER BY " + orderBy;
		}

		return statement.executeQuery(query);
	}

	private Gson createSerializer() {
		Map<String, Class<? extends SchemaOperation>> operations = Maps.newLinkedHashMap();
		operations.put(AddColumn.class.getSimpleName(), AddColumn.class);
		operations.put(AddForeignKey.class.getSimpleName(), AddForeignKey.class);
		operations.put(AlterColumn.class.getSimpleName(), AlterColumn.class);
		operations.put(CopyTable.class.getSimpleName(), CopyTable.class);
		operations.put(CreateIndex.class.getSimpleName(), CreateIndex.class);
		operations.put(CreateTable.class.getSimpleName(), CreateTable.class);
		operations.put(DecomposeTable.class.getSimpleName(), DecomposeTable.class);
		operations.put(DropColumn.class.getSimpleName(), DropColumn.class);
		operations.put(DropIndex.class.getSimpleName(), DropIndex.class);
		operations.put(DropForeignKey.class.getSimpleName(), DropForeignKey.class);
		operations.put(DropTable.class.getSimpleName(), DropTable.class);
		operations.put(JoinTable.class.getSimpleName(), JoinTable.class);
		operations.put(MergeTable.class.getSimpleName(), MergeTable.class);
		operations.put(PartitionTable.class.getSimpleName(), PartitionTable.class);
		operations.put(RenameTable.class.getSimpleName(), RenameTable.class);

		JsonDeserializer<ColumnType> columnTypeDeserializer = new JsonDeserializer<ColumnType>() {
			@Override
			public ColumnType deserialize(JsonElement json, Type typeOfT,
					JsonDeserializationContext context) throws JsonParseException {

				JsonObject object = json.getAsJsonObject();
				String type = object.getAsJsonPrimitive("type").getAsString();
				Integer length = null;

				if (type.contains("(")) {
					String part = type.substring(type.indexOf('(') + 1, type.length() - 1);
					length = Integer.parseInt(part);
					type = type.substring(0, type.indexOf('('));
				}

				return PostgresTypes.from(type, length);
			}
		};

		JsonSerializer<ColumnType> columnTypeSerializer = new JsonSerializer<ColumnType>() {
			@Override
			public JsonElement serialize(ColumnType src, Type typeOfSrc, JsonSerializationContext context) {
				JsonObject object = new JsonObject();
				object.addProperty("type", src.getNotation());
				return object;
			}
		};

		Gson defaultSerializer = new GsonBuilder()
				.registerTypeAdapter(ColumnType.class, columnTypeDeserializer)
				.registerTypeAdapter(ColumnType.class, columnTypeSerializer)
				.create();

		GsonBuilder builder = new GsonBuilder()
				.registerTypeAdapter(ColumnType.class, columnTypeDeserializer)
				.registerTypeAdapter(ColumnType.class, columnTypeSerializer)
				.registerTypeAdapter(SchemaOperation.class, new JsonDeserializer<SchemaOperation>() {
					@Override
					public SchemaOperation deserialize(JsonElement json, Type typeOfT,
							JsonDeserializationContext context) throws JsonParseException {

						JsonObject object = json.getAsJsonObject();
						String type = object.getAsJsonPrimitive("type").getAsString();
						Class<? extends SchemaOperation> operationType = operations.get(type);
						return context.deserialize(json, operationType);
					}
				});

		for (Map.Entry<String, Class<? extends SchemaOperation>> entry : operations.entrySet()) {
			builder.registerTypeAdapter(entry.getValue(), new JsonSerializer<SchemaOperation>() {
				@Override
				public JsonElement serialize(SchemaOperation src, Type typeOfSrc, JsonSerializationContext context) {
					JsonObject object = (JsonObject) defaultSerializer.toJsonTree(src);
					object.addProperty("type", entry.getKey());
					return object;
				}
			});
		}

		return builder.create();
	}
}
