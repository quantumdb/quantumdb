package io.quantumdb.core.schema.definitions;

import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PostgresTypes {

	public static ColumnType from(String input) {
		Pattern pattern = Pattern.compile("^(.+)\\(([0-9]+)\\)$");
		Matcher match = pattern.matcher(input);
		if (match.find()) {
			String type = match.group(1);
			int length = Integer.parseInt(match.group(2));
			return from(type, length);
		}
		else {
			return from(input, null);
		}
	}

	public static ColumnType from(String type, Integer length) {
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
			case "timestamp":
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

	public static ColumnType oid() {
		return new ColumnType(ColumnType.Type.OID, false, "oid", () -> 0L,
				(statement, position, value) -> statement.setLong(position, ((Number) value).longValue()));
	}

	public static ColumnType uuid() {
		return new ColumnType(ColumnType.Type.UUID, true, "uuid", () -> UUID.randomUUID(),
				(statement, position, value) -> statement.setObject(position, value));
	}

	public static ColumnType varchar(int length) {
		return new ColumnType(ColumnType.Type.VARCHAR, true, "varchar(" + length + ")", () -> "",
				(statement, position, value) -> statement.setString(position, value.toString()));
	}

	public static ColumnType chars(int length) {
		return new ColumnType(ColumnType.Type.CHAR, true, "char(" + length + ")", () -> "",
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

	public static ColumnType timestamp(boolean withTimezone) {
		String typeNotation = "timestamp" + (withTimezone ? " with time zone" : "");
		return new ColumnType(ColumnType.Type.TIMESTAMP, true, typeNotation, () -> new Timestamp(new Date().getTime()),
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
