package io.quantumdb.core.schema.definitions;

import java.sql.Timestamp;
import java.util.Date;

public class TestTypes {

	public static ColumnType from(String type, Integer length) {
		switch (type.toLowerCase()) {
			case "smallint":
				return TestTypes.smallint();
			case "bigint":
				return TestTypes.bigint();
			case "integer":
				return TestTypes.integer();
			case "bool":
			case "boolean":
				return TestTypes.bool();
			case "varchar":
				return TestTypes.varchar(length);
			case "text":
				return TestTypes.text();
			case "timestamp":
				return TestTypes.timestamp();
			case "date":
				return TestTypes.date();
			case "double":
				return TestTypes.floats();
			case "float":
				return TestTypes.doubles();
			default:
				String error = "Unsupported type: " + type;
				if (length != null) {
					error += " (length: " + length + ")";
				}
				throw new IllegalArgumentException(error);
		}
	}

	public static ColumnType varchar(int length) {
		return new ColumnType(ColumnType.Type.VARCHAR, true, "varchar(" + length + ")", () -> "",
				(statement, position, value) -> statement.setString(position, value.toString()));
	}

	public static ColumnType text() {
		return new ColumnType(ColumnType.Type.TEXT, true, "text", () -> "",
				(statement, position, value) -> statement.setString(position, value.toString()));
	}

	public static ColumnType bool() {
		return new ColumnType(ColumnType.Type.BOOLEAN, false, "boolean", () -> false,
				(statement, position, value) -> statement.setBoolean(position, (Boolean) value));
	}

	public static ColumnType smallint() {
		return new ColumnType(ColumnType.Type.SMALLINT, false, "smallint", () -> 0,
				(statement, position, value) -> statement.setInt(position, ((Number) value).intValue()));
	}

	public static ColumnType integer() {
		return new ColumnType(ColumnType.Type.INTEGER, false, "integer", () -> 0,
				(statement, position, value) -> statement.setInt(position, ((Number) value).intValue()));
	}

	public static ColumnType bigint() {
		return new ColumnType(ColumnType.Type.BIGINT, false, "bigint", () -> 0L,
				(statement, position, value) -> statement.setLong(position, ((Number) value).longValue()));
	}

	public static ColumnType timestamp() {
		return new ColumnType(ColumnType.Type.TIMESTAMP, true, "timestamp", () -> new Timestamp(new Date().getTime()),
				(statement, position, value) -> statement.setTimestamp(position, (Timestamp) value));
	}

	public static ColumnType date() {
		return new ColumnType(ColumnType.Type.DATE, true, "date", () -> new java.sql.Date(new Date().getTime()),
				(statement, position, value) -> statement.setDate(position, (java.sql.Date) value));
	}

	public static ColumnType doubles() {
		return new ColumnType(ColumnType.Type.DOUBLE, false, "real", () -> 0.00d,
				(statement, position, value) -> statement.setDouble(position, (Double) value));
	}

	public static ColumnType floats() {
		return new ColumnType(ColumnType.Type.FLOAT, false, "double precision", () -> 0.00f,
				(statement, position, value) -> statement.setFloat(position, (Float) value));
	}

}
