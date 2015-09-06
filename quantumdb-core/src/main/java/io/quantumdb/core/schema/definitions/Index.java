package io.quantumdb.core.schema.definitions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.List;

import com.google.common.collect.ImmutableList;
import io.quantumdb.core.utils.RandomHasher;
import lombok.Data;

@Data
public class Index {

	private final String indexName;
	private final ImmutableList<String> columns;
	private final boolean unique;

	private Table parent;

	public Index(List<String> columns, boolean unique) {
		this("idx_" + RandomHasher.generateHash(), columns, unique);
	}

	public Index(String indexName, List<String> columns, boolean unique) {
		checkArgument(!isNullOrEmpty(indexName), "You must specify a 'foreignKeyName'.");
		checkArgument(columns != null && !columns.isEmpty(), "You must specify at least one column.");

		this.indexName = indexName;
		this.columns = ImmutableList.copyOf(columns);
		this.unique = unique;
	}

	@Override
	public String toString() {
		return PrettyPrinter.prettyPrint(this);
	}

}
