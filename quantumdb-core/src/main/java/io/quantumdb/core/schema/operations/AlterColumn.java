package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.quantumdb.core.schema.definitions.Column.Hint;
import io.quantumdb.core.schema.definitions.ColumnType;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;
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

	@Setter(AccessLevel.NONE)
	private Optional<String> newColumnName;

	@Setter(AccessLevel.NONE)
	private Optional<ColumnType> newColumnType;

	@Setter(AccessLevel.NONE)
	private Optional<String> newDefaultValueExpression;

	AlterColumn(String tableName, String columnName) {
		checkArgument(!isNullOrEmpty(tableName), "You must specify a 'tableName'");
		checkArgument(!isNullOrEmpty(columnName), "You must specify a 'columnName'");

		this.tableName = tableName;
		this.columnName = columnName;
		this.hintsToDrop = Sets.newHashSet();
		this.hintsToAdd = Sets.newHashSet();

		this.newColumnName = Optional.empty();
		this.newColumnType = Optional.empty();
		this.newDefaultValueExpression = Optional.empty();
	}

	public AlterColumn rename(String newColumnName) {
		checkArgument(!isNullOrEmpty(newColumnName), "You must specify a 'newColumnNameName'.");

		this.newColumnName = Optional.of(newColumnName);
		return this;
	}

	public AlterColumn modifyDataType(ColumnType newColumnType) {
		checkArgument(newColumnType != null, "You must specify a 'newColumnType'.");

		this.newColumnType = Optional.of(newColumnType);
		return this;
	}

	public AlterColumn modifyDefaultExpression(String newDefaultExpression) {
		checkArgument(!isNullOrEmpty(newDefaultExpression), "You must specify a 'newDefaultExpression'.");

		this.newDefaultValueExpression = Optional.of(newDefaultExpression);
		return this;
	}

	public AlterColumn dropDefaultExpression() {
		this.newDefaultValueExpression = Optional.ofNullable("");
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

	public ImmutableSet<Hint> getHintsToDrop() {
		return ImmutableSet.copyOf(hintsToDrop);
	}

	public ImmutableSet<Hint> getHintsToAdd() {
		return ImmutableSet.copyOf(hintsToAdd);
	}
	
}
