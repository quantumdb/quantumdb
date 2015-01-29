package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class PartitionTable implements SchemaOperation {

	private final String tableName;
	private final Map<String, String> partitions;

	PartitionTable(String tableName) {
		checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a 'tableName'.");

		this.tableName = tableName;
		this.partitions = Maps.newLinkedHashMap();
	}
	
	public PartitionTable into(String tableName, String expression) {
		checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a 'tableName'.");
		checkArgument(!Strings.isNullOrEmpty(expression), "You must specify a 'expression'.");
		checkArgument(!partitions.containsKey(tableName), "You cannot decompose into the same table multiple times.");

		partitions.put(tableName, expression);
		return this;
	}

	public ImmutableMap<String, String> getPartitions() {
		return ImmutableMap.copyOf(partitions);
	}

}
