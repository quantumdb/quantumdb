package io.quantumdb.core.backends.postgresql;

import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;

import io.quantumdb.core.schema.definitions.DataType;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PostgresTypes {

	public static DataType from(String type, Integer length) {
		switch (type.toLowerCase()) {
			case "oid":
				return PostgresTypes.oid();
			case "uuid":
				return PostgresTypes.uuid();
			case "smallint":
				return PostgresTypes.smallint();
			case "bigint":
				return PostgresTypes.bigint();
			case "integer":
				return PostgresTypes.integer();
			case "bool":
			case "boolean":
				return PostgresTypes.bool();
			case "varchar":
			case "character varying":
				return PostgresTypes.varchar(length);
			case "character":
				return PostgresTypes.chars(length);
			case "text":
				return PostgresTypes.text();
			case "timestamp with time zone":
				return PostgresTypes.timestamp(true);
			case "timestamp without time zone":
				return PostgresTypes.timestamp(false);
			case "date":
				return PostgresTypes.date();
			case "double precision":
				return PostgresTypes.floats();
			case "real":
				return PostgresTypes.doubles();
			default:
				String error = "Unsupported type: " + type;
				if (length != null) {
					error += " (length: " + length + ")";
				}
				throw new IllegalArgumentException(error);
		}
	}

	public static DataType oid() {
		return new DataType(DataType.Type.OID, false, "oid", () -> 0L,
				(statement, position, value) -> statement.setLong(position, ((Number) value).longValue()));
	}

	public static DataType uuid() {
		return new DataType(DataType.Type.UUID, true, "uuid", () -> UUID.randomUUID(),
				(statement, position, value) -> statement.setObject(position, value));
	}

	public static DataType varchar(int length) {
		return new DataType(DataType.Type.VARCHAR, true, "varchar(" + length + ")", () -> "",
				(statement, position, value) -> statement.setString(position, value.toString()));
	}

	public static DataType chars(int length) {
		return new DataType(DataType.Type.CHAR, true, "char(" + length + ")", () -> "",
				(statement, position, value) -> statement.setString(position, value.toString()));
	}

	public static DataType text() {
		return new DataType(DataType.Type.TEXT, true, "text", () -> "",
				(statement, position, value) -> statement.setString(position, value.toString()));
	}
	
	public static DataType bool() {
		return new DataType(DataType.Type.BOOLEAN, false, "boolean", () -> false,
				(statement, position, value) -> statement.setBoolean(position, (Boolean) value));
	}
	
	public static DataType smallint() {
		return new DataType(DataType.Type.SMALLINT, false, "smallint", () -> 0,
				(statement, position, value) -> statement.setInt(position, ((Number) value).intValue()));
	}
	
	public static DataType integer() {
		return new DataType(DataType.Type.INTEGER, false, "integer", () -> 0,
				(statement, position, value) -> statement.setInt(position, ((Number) value).intValue()));
	}
	
	public static DataType bigint() {
		return new DataType(DataType.Type.BIGINT, false, "bigint", () -> 0L,
				(statement, position, value) -> statement.setLong(position, ((Number) value).longValue()));
	}

	public static DataType timestamp(boolean withTimezone) {
		String typeNotation = "timestamp" + (withTimezone ? " with time zone" : "");
		return new DataType(DataType.Type.TIMESTAMP, true, typeNotation, () -> new Timestamp(new Date().getTime()),
				(statement, position, value) -> statement.setTimestamp(position, (Timestamp) value));
	}

	public static DataType date() {
		return new DataType(DataType.Type.DATE, true, "date", () -> new java.sql.Date(new Date().getTime()),
				(statement, position, value) -> statement.setDate(position, (java.sql.Date) value));
	}

	public static DataType doubles() {
		return new DataType(DataType.Type.DOUBLE, false, "real", () -> 0.00d,
				(statement, position, value) -> statement.setDouble(position, (Double) value));
	}

	public static DataType floats() {
		return new DataType(DataType.Type.FLOAT, false, "double precision", () -> 0.00f,
				(statement, position, value) -> statement.setFloat(position, (Float) value));
	}

	public static DataType trigger() {
		return new DataType(DataType.Type.TRIGGER, false, "trigger", () -> null, (statement, position, value) -> {});
	}
	
}
