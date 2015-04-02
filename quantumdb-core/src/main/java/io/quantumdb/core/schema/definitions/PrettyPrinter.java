package io.quantumdb.core.schema.definitions;

import java.lang.reflect.Type;

import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.quantumdb.core.schema.definitions.Column.Hint;

class PrettyPrinter {

	private static final Gson gson = new GsonBuilder()
			.registerTypeAdapter(Catalog.class, new JsonSerializer<Catalog>() {
				@Override
				public JsonElement serialize(Catalog src, Type typeOfSrc, JsonSerializationContext context) {
					JsonObject model = new JsonObject();
					model.addProperty("catalogName", src.getName());

					JsonArray tables = new JsonArray();
					src.getTables().stream()
							.map(context::serialize)
							.forEachOrdered(tables::add);

					JsonArray sequences = new JsonArray();
					src.getSequences().stream()
							.map(context::serialize)
							.forEachOrdered(sequences::add);

					model.add("tables", tables);
					model.add("sequences", sequences);
					return model;
				}
			})
			.registerTypeAdapter(Column.class, new JsonSerializer<Column>() {
				@Override
				public JsonElement serialize(Column src, Type typeOfSrc, JsonSerializationContext context) {
					JsonObject model = new JsonObject();
					model.addProperty("columnName", src.getName());
					model.addProperty("type", src.getType().getNotation());
					model.addProperty("defaultValue", src.getDefaultValue());

					if (!src.getHints().isEmpty()) {
						JsonArray elements = new JsonArray();
						src.getHints().stream()
								.map(Hint::name)
								.map(JsonPrimitive::new)
								.forEachOrdered(elements::add);

						model.add("hints", elements);
					}
					return model;
				}
			})
			.registerTypeAdapter(ForeignKey.class, new JsonSerializer<ForeignKey>() {
				@Override
				public JsonElement serialize(ForeignKey src, Type typeOfSrc, JsonSerializationContext context) {
					JsonObject model = new JsonObject();
					model.addProperty("referringFrom", src.getReferencingTableName() + ": " + Joiner.on(", ").join(src.getReferencingColumns()));
					model.addProperty("referringTo", src.getReferredTableName() + ": " + Joiner.on(", ").join(src.getReferredColumns()));
					return model;
				}
			})
			.registerTypeAdapter(Sequence.class, new JsonSerializer<Sequence>() {
				@Override
				public JsonElement serialize(Sequence src, Type typeOfSrc, JsonSerializationContext context) {
					JsonObject model = new JsonObject();
					model.addProperty("sequenceName", src.getName());
					return model;
				}
			})
			.registerTypeAdapter(Table.class, new JsonSerializer<Table>() {
				@Override
				public JsonElement serialize(Table src, Type typeOfSrc, JsonSerializationContext context) {
					JsonObject model = new JsonObject();
					model.addProperty("tableName", src.getName());

					JsonArray columns = new JsonArray();
					src.getColumns().stream()
							.map(context::serialize)
							.forEachOrdered(columns::add);

					JsonArray foreignKeys = new JsonArray();
					src.getForeignKeys().stream()
							.map(context::serialize)
							.forEachOrdered(foreignKeys::add);

					model.add("columns", columns);
					model.add("foreignKeys", foreignKeys);
					return model;
				}
			})
			.setPrettyPrinting()
			.create();

	public static String prettyPrint(Object object) {
		return gson.toJson(object);
	}

}
