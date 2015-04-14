package io.quantumdb.core.backends.postgresql.migrator;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.quantumdb.core.backends.postgresql.planner.Migration;
import io.quantumdb.core.backends.postgresql.planner.MigrationPlan;
import io.quantumdb.core.backends.postgresql.planner.Step;
import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;
import lombok.Data;

@Data
public class Expansion {

	private final Version origin;
	private final Version target;

	private final List<Step> migrationSteps;
	private final DataMappings dataMappings;
	private final List<String> tableIdsWithNullRecords;
	private final State state;

	public Expansion(MigrationPlan migrationPlan, State state, Version origin, Version target) {
		this.origin = origin;
		this.target = target;
		this.migrationSteps = migrationPlan.getSteps();
		this.dataMappings = migrationPlan.getDataMappings();
		this.tableIdsWithNullRecords = migrationPlan.getTableIdsWithNullRecords();
		this.state = state;
	}

	public Set<String> getTableIds() {
		return migrationSteps.stream()
				.flatMap(migration -> migration.getTableMigrations().stream())
				.map(Migration::getTableName)
				.collect(Collectors.toSet());
	}

}
