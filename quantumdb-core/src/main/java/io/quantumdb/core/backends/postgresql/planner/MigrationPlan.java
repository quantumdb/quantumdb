package io.quantumdb.core.backends.postgresql.planner;

import java.util.List;

import com.google.common.collect.ImmutableList;
import io.quantumdb.core.migration.utils.DataMappings;
import lombok.Data;

@Data
public class MigrationPlan {

	private final ImmutableList<Step> steps;
	private final ImmutableList<String> createNullObjectsForTables;
	private final DataMappings dataMappings;

	MigrationPlan(List<Step> steps, List<String> createNullObjectsForTables, DataMappings dataMappings) {
		this.steps = ImmutableList.copyOf(steps);
		this.createNullObjectsForTables = ImmutableList.copyOf(createNullObjectsForTables);
		this.dataMappings = dataMappings;
	}

}
