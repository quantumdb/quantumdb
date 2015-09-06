package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.Data;
import lombok.experimental.Accessors;


/**
 * This SchemaOperation describes an operation which adds a non-existent column to an already existing table.
 */
@Data
@Accessors(chain = true)
public class CreateIndex implements SchemaOperation {

	private final String tableName;
	private final boolean unique;
	private final List<String> columns;

	CreateIndex(String tableName, boolean unique, String... columns) {
		checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a 'tableName'");
		checkArgument(columns.length > 0, "You must specify at least one column");

		this.tableName = tableName;
		this.unique = unique;
		this.columns = Lists.newArrayList(columns);
	}

	public ImmutableList<String> getColumns() {
		return ImmutableList.copyOf(columns);
	}
	
}
