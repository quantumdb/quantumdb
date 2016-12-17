package io.quantumdb.core.schema.definitions;

import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import io.quantumdb.core.schema.definitions.Column.Hint;

class PrettyPrinter {

	private static final Gson gson = new GsonBuilder()
			.registerTypeAdapter(Catalog.class, (JsonSerializer<Catalog>) (src, typeOfSrc, context) -> {
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
			})
			.registerTypeAdapter(Column.class, (JsonSerializer<Column>) (src, typeOfSrc, context) -> {
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
			})
			.registerTypeAdapter(ForeignKey.class, (JsonSerializer<ForeignKey>) (src, typeOfSrc, context) -> {
				JsonObject mapping = new JsonObject();
				src.getColumnMapping().entrySet()
						.forEach(entry -> mapping.addProperty(entry.getKey(), entry.getValue()));

				JsonObject model = new JsonObject();
				model.addProperty("name", src.getForeignKeyName());
				model.addProperty("from", src.getReferencingTableName());
				model.addProperty("to", src.getReferredTableName());
				model.add("mapping", mapping);
				model.addProperty("onUpdate", src.getOnUpdate().name());
				model.addProperty("onDelete", src.getOnDelete().name());
				return model;
			})
			.registerTypeAdapter(Sequence.class, (JsonSerializer<Sequence>) (src, typeOfSrc, context) -> {
				JsonObject model = new JsonObject();
				model.addProperty("sequenceName", src.getName());
				return model;
			})
			.registerTypeAdapter(Index.class, (JsonSerializer<Index>) (src, typeOfSrc, context) -> {
				JsonObject model = new JsonObject();
				model.addProperty("indexName", src.getIndexName());
				model.addProperty("table", src.getParent().getName());
				model.addProperty("unique", src.isUnique());
				model.addProperty("columns", Joiner.on(", ").join(src.getColumns()));
				return model;
			})
			.registerTypeAdapter(Table.class, (JsonSerializer<Table>) (src, typeOfSrc, context) -> {
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
			})
			.setPrettyPrinting()
			.create();

	public static String prettyPrint(Object object) {
		return gson.toJson(object);
	}

}
