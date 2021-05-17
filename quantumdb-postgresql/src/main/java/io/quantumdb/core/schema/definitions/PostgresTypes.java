package io.quantumdb.core.schema.definitions;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PostgresTypes {

	public static ColumnType fromString(String input) {
		Matcher match = Pattern.compile("^(.+)\\(([0-9]+),([0-9]+)\\)$").matcher(input);
		if (match.find()) {
			String type = match.group(1);
			int precision = Integer.parseInt(match.group(2));
			int scale = Integer.parseInt(match.group(3));
			return from(type.trim(), precision, scale);
		} else {
			match = Pattern.compile("^(.+)\\(([0-9]+)\\)$").matcher(input);
			if (match.find()) {
				String type = match.group(1);
				int length = Integer.parseInt(match.group(2));
				return from(type.trim(), length);
			}
		}
		return from(input.trim());
	}

	public static ColumnType from(String type) {
		switch (type.toLowerCase()) {
			case "oid":
				return PostgresTypes.oid();
			case "uuid":
				return PostgresTypes.uuid();
			case "smallserial":
			case "int2":
			case "smallint":
				return PostgresTypes.smallint();
			case "bigserial":
			case "int8":
			case "bigint":
				return PostgresTypes.bigint();
			case "serial":
			case "int":
			case "int4":
			case "integer":
				return PostgresTypes.integer();
			case "decimal":
			case "numeric":
				return PostgresTypes.numeric();
			case "bool":
			case "boolean":
				return PostgresTypes.bool();
			case "varchar":
			case "character varying":
				return PostgresTypes.varchar();
			case "char":
			case "character":
				return PostgresTypes.chars(1);
			case "text":
				return PostgresTypes.text();
			case "timestamp with time zone":
				return PostgresTypes.timestamp(true);
			case "timestamp":
			case "timestamp without time zone":
				return PostgresTypes.timestamp(false);
			case "date":
				return PostgresTypes.date();
			case "float8":
			case "double precision":
				return PostgresTypes.floats();
			case "float4":
			case "real":
				return PostgresTypes.doubles();
			case "byte array":
			case "bytea":
				return PostgresTypes.bytea();
			default:
				String error = "Unsupported type: " + type;
				throw new IllegalArgumentException(error);
		}
	}

	public static ColumnType from(String type, Integer length) {
		if (length == 0) {
			return from(type);
		}
		switch (type.toLowerCase()) {
			case "smallserial":
			case "int2":
			case "smallint":
				return PostgresTypes.smallint();
			case "bigserial":
			case "int8":
			case "bigint":
				return PostgresTypes.bigint();
			case "serial":
			case "int":
			case "int4":
			case "integer":
				return PostgresTypes.integer();
			case "float8":
			case "double precision":
				return PostgresTypes.floats();
			case "float4":
			case "real":
				return PostgresTypes.doubles();
			case "decimal":
			case "numeric":
				return PostgresTypes.numeric(length);
			case "varchar":
			case "character varying":
				return PostgresTypes.varchar(length);
			case "char":
			case "character":
				return PostgresTypes.chars(length);
			case "timestamp with time zone":
				return PostgresTypes.timestamp(true, length);
			case "timestamp":
			case "timestamp without time zone":
				return PostgresTypes.timestamp(false, length);
			default:
				String error = "Unsupported type: " + type;
				error += " (length: " + length + ")";
				throw new IllegalArgumentException(error);
		}
	}

	public static ColumnType from(String type, Integer precision, Integer scale) {
		if (scale == 0) {
			return from(type, precision);
		}
		switch (type.toLowerCase()) {
			case "decimal":
			case "numeric":
				return PostgresTypes.numeric(precision, scale);
			default:
				String error = "Unsupported type: " + type;
				error += " (1st argument: " + precision + ", 2nd argument: " + scale + ")";
				throw new IllegalArgumentException(error);
		}
	}

	public static ColumnType oid() {
		return new ColumnType(ColumnType.Type.OID, false, "oid", () -> 0L,
				(statement, position, value) -> statement.setLong(position, ((Number) value).longValue()));
	}

	public static ColumnType uuid() {
		return new ColumnType(ColumnType.Type.UUID, true, "uuid", UUID::randomUUID,
				(statement, position, value) -> statement.setObject(position, value));
	}

	public static ColumnType varchar(int length) {
		return new ColumnType(ColumnType.Type.VARCHAR, true, "varchar(" + length + ")", () -> "",
				(statement, position, value) -> statement.setString(position, value.toString()));
	}

	public static ColumnType varchar() {
		return new ColumnType(ColumnType.Type.VARCHAR, true, "varchar", () -> "",
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

	public static ColumnType numeric(Integer precision, Integer scale) {
		return new ColumnType(ColumnType.Type.NUMERIC, false, "numeric(" + precision + "," + scale + ")", () -> BigDecimal.valueOf(0.00d),
				(statement, position, value) -> statement.setBigDecimal(position, new BigDecimal(value.toString())));
	}
	public static ColumnType numeric(Integer precision) {
		return new ColumnType(ColumnType.Type.NUMERIC, false, "numeric(" + precision + ")", () -> BigDecimal.valueOf(0.00d),
				(statement, position, value) -> statement.setBigDecimal(position, new BigDecimal(value.toString())));
	}

	public static ColumnType numeric() {
		return new ColumnType(ColumnType.Type.NUMERIC, false, "numeric", () -> BigDecimal.valueOf(0.00d),
				(statement, position, value) -> statement.setBigDecimal(position, new BigDecimal(value.toString())));
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

	public static ColumnType timestamp(boolean withTimezone, Integer length) {
		String typeNotation = "timestamp (" + length + ")" + (withTimezone ? " with time zone" : "");
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

	public static ColumnType bytea() {
		return new ColumnType(ColumnType.Type.BYTEA, false, "bytea", () -> new byte[0],
				(statement, position, value) -> statement.setBytes(position, (byte[]) value));
	}

}
