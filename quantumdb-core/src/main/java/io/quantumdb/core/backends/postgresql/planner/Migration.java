package io.quantumdb.core.backends.postgresql.planner;

import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import lombok.Data;

@Data
public class Migration {

	private final String tableName;
	private final ImmutableSet<String> columnsToMigrateOnSecondPass;

	Migration(String tableName, Set<String> columnsToMigrateOnSecondPass) {
		this.tableName = tableName;
		this.columnsToMigrateOnSecondPass = ImmutableSet.copyOf(columnsToMigrateOnSecondPass);
	}

	public boolean requiresDoublePass() {
		return !columnsToMigrateOnSecondPass.isEmpty();
	}

	@Override
	public String toString() {
		return tableName + " [" + Joiner.on(", ").join(columnsToMigrateOnSecondPass) + "]";
	}

}
