package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import io.quantumdb.core.schema.definitions.Column.Hint;
import io.quantumdb.core.schema.definitions.ColumnType;
import lombok.Data;
import lombok.experimental.Accessors;


/**
 * This SchemaOperation describes an operation which alters an already existing column.
 */
@Data
@Accessors(chain = true)
public class AlterColumn implements SchemaOperation {

	private final String tableName;
	private final String columnName;
	private final Set<Hint> hintsToDrop;
	private final Set<Hint> hintsToAdd;

	private String newColumnName;
	private ColumnType newColumnType;
	private String newDefaultExpression;
	private boolean dropDefaultExpression;

	AlterColumn(String tableName, String columnName) {
		checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a 'tableName'");
		checkArgument(!Strings.isNullOrEmpty(columnName), "You must specify a 'columnName'");

		this.tableName = tableName;
		this.columnName = columnName;
		this.hintsToDrop = Sets.newHashSet();
		this.hintsToAdd = Sets.newHashSet();
	}

	public AlterColumn rename(String newColumnName) {
		checkArgument(!Strings.isNullOrEmpty(newColumnName), "You must specify a 'newColumnNameName'.");

		this.newColumnName = newColumnName;
		return this;
	}

	public AlterColumn modifyDataType(ColumnType newColumnType) {
		checkArgument(newColumnType != null, "You must specify a 'newColumnType'.");

		this.newColumnType = newColumnType;
		return this;
	}

	public AlterColumn modifyDefaultExpression(String newDefaultExpression) {
		checkArgument(!Strings.isNullOrEmpty(newDefaultExpression), "You must specify a 'newDefaultExpression'.");

		this.newDefaultExpression = newDefaultExpression;
		this.dropDefaultExpression = false;
		return this;
	}

	public AlterColumn dropDefaultExpression() {
		this.newDefaultExpression = null;
		this.dropDefaultExpression = true;
		return this;
	}

	public AlterColumn dropHint(Hint hint) {
		checkArgument(hint != null, "You must specify a 'hint'.");

		this.hintsToDrop.add(hint);
		this.hintsToAdd.remove(hint);
		return this;
	}

	public AlterColumn addHint(Hint hint) {
		checkArgument(hint != null, "You must specify a 'hint'.");

		this.hintsToAdd.add(hint);
		this.hintsToDrop.remove(hint);
		return this;
	}
	
}
