package io.quantumdb.core.schema.definitions;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PostgresTypes {

	public static ColumnType from(String type, Integer length) {
		switch (type) {
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
			default:
				String error = "Unsupported type: " + type;
				if (length != null) {
					error += " (length: " + length + ")";
				}
				throw new IllegalArgumentException(error);
		}
	}

	public static enum Type {
		OID,
		UUID,
		CHAR,
		VARCHAR,
		TEXT,
		INT1,
		SMALLINT,
		INTEGER,
		BIGINT,
		FLOAT,
		BOOLEAN,
		DATE,
		TIMESTAMP
		;
	}

	public static ColumnType oid() {
		return new ColumnType(Type.OID, false, "oid");
	}

	public static ColumnType uuid() {
		return new ColumnType(Type.UUID, true, "uid");
	}

	public static ColumnType varchar(int length) {
		return new ColumnType(Type.VARCHAR, true, "varchar(" + length + ")");
	}

	public static ColumnType chars(int length) {
		return new ColumnType(Type.CHAR, true, "char(" + length + ")");
	}

	public static ColumnType text() {
		return new ColumnType(Type.TEXT, true, "text");
	}
	
	public static ColumnType bool() {
		return new ColumnType(Type.BOOLEAN, false, "boolean");
	}
	
	public static ColumnType smallint() {
		return new ColumnType(Type.SMALLINT, false, "smallint");
	}
	
	public static ColumnType integer() {
		return new ColumnType(Type.INTEGER, false, "integer");
	}
	
	public static ColumnType bigint() {
		return new ColumnType(Type.BIGINT, false, "bigint");
	}

	public static ColumnType timestamp(boolean withTimezone) {
		return new ColumnType(Type.TIMESTAMP, true, "timestamp" + (withTimezone ? " with time zone" : ""));
	}

	public static ColumnType date() {
		return new ColumnType(Type.DATE, true, "date");
	}

	public static ColumnType floats() {
		return new ColumnType(Type.FLOAT, false, "double precision");
	}
	
}
