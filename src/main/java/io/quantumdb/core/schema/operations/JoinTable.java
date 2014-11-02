package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;
import java.util.Map;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class JoinTable implements SchemaOperation {

	private final Map<String, String> sourceTables;
	private final Multimap<String, String> sourceColumns;
	private final Map<String, String> joinConditions;

	@Setter(AccessLevel.NONE)
	private String targetTableName;

	JoinTable(String sourceTable, String alias, String... columns) {
		checkArgument(!Strings.isNullOrEmpty(sourceTable), "You must specify a 'sourceTable'.");
		checkArgument(!Strings.isNullOrEmpty(alias), "You must specify a 'alias'.");

		this.sourceTables = Maps.newLinkedHashMap();
		this.sourceColumns = LinkedHashMultimap.create();
		this.joinConditions = Maps.newLinkedHashMap();

		sourceTables.put(alias, sourceTable);
		sourceColumns.putAll(alias, Arrays.asList(columns));
	}

	public ImmutableMap<String, String> getSourceTables() {
		return ImmutableMap.copyOf(sourceTables);
	}

	public ImmutableMultimap<String, String> getSourceColumns() {
		return ImmutableMultimap.copyOf(sourceColumns);
	}

	public ImmutableMap<String, String> getJoinConditions() {
		return ImmutableMap.copyOf(joinConditions);
	}

	public JoinTable with(String sourceTable, String alias, String joinCondition, String... columns) {
		checkArgument(!Strings.isNullOrEmpty(sourceTable), "You must specify a 'sourceTable'.");
		checkArgument(!Strings.isNullOrEmpty(alias), "You must specify a 'alias'.");
		checkArgument(!Strings.isNullOrEmpty(joinCondition), "You must specify a 'joinCondition'.");
		checkArgument(!sourceTables.containsKey(alias), "You have ambiguously used the alias: '" + alias + "'.");

		this.sourceTables.put(alias, sourceTable);
		this.sourceColumns.putAll(alias, Arrays.asList(columns));
		this.joinConditions.put(alias, joinCondition);
		return this;
	}

	public JoinTable into(String tableName) {
		checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a 'tableName'.");
		checkArgument(targetTableName == null, "You have already specified a target table.");
		checkArgument(!sourceColumns.isEmpty(), "You must specify which columns to join into the target table.");

		this.targetTableName = tableName;
		return this;
	}

}
