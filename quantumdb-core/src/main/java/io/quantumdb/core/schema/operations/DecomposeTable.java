package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import lombok.Data;
import lombok.experimental.Accessors;


/**
 * This SchemaOperation describes an operation which creates one or more tables consisting of specific columns from
 * the source table.
 */
@Data
@Accessors(chain = true)
public class DecomposeTable implements SchemaOperation {

	private final String tableName;
	private final Multimap<String, String> decompositions;

	DecomposeTable(String tableName) {
		checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a 'tableName'.");

		this.tableName = tableName;
		this.decompositions = LinkedHashMultimap.create();
	}

	public DecomposeTable into(String tableName, String... columns) {
		checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a 'tableName'.");
		checkArgument(columns.length != 0, "You must specify at least one column to decompose.");

		decompositions.putAll(tableName, Arrays.asList(columns));
		return this;
	}

	public ImmutableMultimap<String, String> getDecompositions() {
		return ImmutableMultimap.copyOf(decompositions);
	}

}
