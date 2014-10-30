package io.quantumdb.core.schema.definitions;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GenericColumnTypes {
	
	public static enum Type {
		VARCHAR,
		INT1,
		INT2,
		INT4,
		INT8, 
		BOOLEAN,
		TIMESTAMP
		;
	}
	
	public static ColumnType varchar(int length) {
		return new ColumnType(Type.VARCHAR, length, null, null);
	}
	
	public static ColumnType bool() {
		return new ColumnType(Type.BOOLEAN, null, null, null);
	}
	
	public static ColumnType int2() {
		return new ColumnType(Type.INT2, null, null, null);
	}
	
	public static ColumnType int4() {
		return new ColumnType(Type.INT4, null, null, null);
	}
	
	public static ColumnType int8() {
		return new ColumnType(Type.INT8, null, null, null);
	}

	public static ColumnType timestamp(boolean withTimezone) {
		return new ColumnType(Type.TIMESTAMP, null, null, withTimezone);
	}
	
}
