package io.quantumdb.core.backends.postgresql.planner;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import lombok.Data;

@Data
public class Step {

	private final ImmutableSet<Migration> tableMigrations;

	Step(Migration... tableMigrations) {
		Set<Migration> set = Sets.newHashSet(tableMigrations);
		checkArgument(set.size() == tableMigrations.length, "You may only specify one object per table name.");
		this.tableMigrations = ImmutableSet.copyOf(tableMigrations);
	}

	Step(Set<Migration> tableMigrations) {
		this.tableMigrations = ImmutableSet.copyOf(tableMigrations);
	}

	@Override
	public String toString() {
		return tableMigrations.toString();
	}

}
