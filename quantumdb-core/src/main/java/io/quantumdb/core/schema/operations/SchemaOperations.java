package io.quantumdb.core.schema.operations;

import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.ColumnType;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SchemaOperations {

	public static CreateTable createTable(String tableName) {
		return new CreateTable(tableName);
	}

	public static DropTable dropTable(String tableName) {
		return new DropTable(tableName);
	}

	public static RenameTable renameTable(String tableName, String newTableName) {
		return new RenameTable(tableName, newTableName);
	}

	public static CopyTable copyTable(String sourceTableName, String targetTableName) {
		return new CopyTable(sourceTableName, targetTableName);
	}

	public static MergeTable mergeTable(String leftTableName, String rightTableName, String targetTableName) {
		return new MergeTable(leftTableName, rightTableName, targetTableName);
	}

	public static PartitionTable partitionTable(String tableName) {
		return new PartitionTable(tableName);
	}

	public static DecomposeTable decomposeTable(String tableName) {
		return new DecomposeTable(tableName);
	}

	public static JoinTable joinTable(String sourceTable, String alias, String... columns) {
		return new JoinTable(sourceTable, alias, columns);
	}

	public static AddColumn addColumn(String tableName, String columnName, ColumnType type, Column.Hint... hints) {
		return addColumn(tableName, columnName, type, null, hints);
	}

	public static AddColumn addColumn(String tableName, String columnName, ColumnType type, String defaultExpression,
			Column.Hint... hints) {
		return new AddColumn(tableName, columnName, type, defaultExpression, hints);
	}

	public static AlterColumn alterColumn(String tableName, String columnName) {
		return new AlterColumn(tableName, columnName);
	}

	public static DropColumn dropColumn(String tableName, String columnName) {
		return new DropColumn(tableName, columnName);
	}

	public static AddForeignKey addForeignKey(String table, String... columns) {
		return new AddForeignKey(table, columns);
	}

}
