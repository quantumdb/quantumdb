package io.quantumdb.core.backends.postgresql.migrator;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.quantumdb.core.backends.postgresql.planner.Migration;
import io.quantumdb.core.backends.postgresql.planner.MigrationPlan;
import io.quantumdb.core.backends.postgresql.planner.Step;
import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.versioning.State;
import lombok.Data;

@Data
public class Expansion {

	private final List<Step> migrationSteps;
	private final DataMappings dataMappings;
	private final List<String> createNullObjectsForTables;
	private final State state;

	public Expansion(MigrationPlan migrationPlan, State state) {
		this.migrationSteps = migrationPlan.getSteps();
		this.dataMappings = migrationPlan.getDataMappings();
		this.createNullObjectsForTables = migrationPlan.getCreateNullObjectsForTables();
		this.state = state;
	}

	public Set<String> getTableIds() {
		return migrationSteps.stream()
				.flatMap(migration -> migration.getTableMigrations().stream())
				.map(Migration::getTableName)
				.collect(Collectors.toSet());
	}

}
