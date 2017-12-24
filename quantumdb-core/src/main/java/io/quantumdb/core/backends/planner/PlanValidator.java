package io.quantumdb.core.backends.planner;

import static com.google.common.base.Preconditions.checkState;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import io.quantumdb.core.backends.planner.Operation.Type;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Table;

public class PlanValidator {

	public static void validate(Plan plan) {
		verifyThatAddNullStepsDoNotDependOnOtherSteps(plan);
		verifyThatAllNullRecordsAreBothAddedAndDropped(plan);
		verifyThatRemoveNullStepIsLastStepIfAddNullStepsArePresent(plan);
		verifyThatAllColumnsAreMigrated(plan);
		verifyThatIdentityColumnsAreMigratedFirst(plan);
		verifyThatNotNullableForeignKeysAreSatisfiedBeforeInitialCopy(plan);
	}

	private static void verifyThatAddNullStepsDoNotDependOnOtherSteps(Plan plan) {
		List<Step> addNullSteps = plan.getSteps().stream()
				.filter(step -> step.getOperation().getType() == Type.ADD_NULL)
				.collect(Collectors.toList());

		addNullSteps.forEach(step -> {
			String message = "Step: " + step + " has a dependency on another step";
			checkState(step.getDependencies().isEmpty(), message);
		});
	}

	private static void verifyThatAllNullRecordsAreBothAddedAndDropped(Plan plan) {
		Set<Table> addNullRecords = plan.getSteps().stream()
				.filter(step -> step.getOperation().getType() == Type.ADD_NULL)
				.map(Step::getOperation)
				.flatMap(operation -> operation.getTables().stream())
				.collect(Collectors.toSet());

		Set<Table> dropNullRecords = plan.getSteps().stream()
				.filter(step -> step.getOperation().getType() == Type.DROP_NULL)
				.map(Step::getOperation)
				.flatMap(operation -> operation.getTables().stream())
				.collect(Collectors.toSet());

		addNullRecords.forEach(table -> {
			String message = "There is no DROP_NULL operation for table: " + table.getName();
			checkState(dropNullRecords.contains(table), message);
			dropNullRecords.remove(table);
		});
		dropNullRecords.forEach(table -> {
			String message = "There is no ADD_NULL operation for table: " + table.getName();
			checkState(addNullRecords.contains(table), message);
		});
	}

	private static void verifyThatRemoveNullStepIsLastStepIfAddNullStepsArePresent(Plan plan) {
		boolean containsAddNullSteps = plan.getSteps().stream()
				.map(Step::getOperation)
				.anyMatch(op -> op.getType() == Type.ADD_NULL);

		if (!containsAddNullSteps) {
			return;
		}

		List<Step> stepsWithNoDependees = Lists.newArrayList(plan.getSteps());
		plan.getSteps().forEach(step -> step.getDependencies().forEach(stepsWithNoDependees::remove));

		checkState(stepsWithNoDependees.size() == 1, "There can only be one last step");

		Operation lastOperation = stepsWithNoDependees.get(0).getOperation();
		checkState(lastOperation.getType() == Type.DROP_NULL, "The last step is not a DROP NULL step");
	}

	private static void verifyThatAllColumnsAreMigrated(Plan plan) {
		Multimap<Table, String> columns = HashMultimap.create();
		plan.getSteps().stream()
				.map(Step::getOperation)
				.filter(op -> op.getType() == Type.COPY)
				.forEach(op -> op.getTables().forEach(table -> columns.putAll(table, op.getColumns())));

		columns.asMap().forEach((k, v) -> {
			Set<String> columnNames = k.getColumns().stream()
					.map(Column::getName)
					.collect(Collectors.toSet());

			checkState(columnNames.equals(v));
		});
	}

	private static void verifyThatIdentityColumnsAreMigratedFirst(Plan plan) {
		for (Step step : plan.getSteps()) {
			Operation operation = step.getOperation();
			if (operation.getType() != Type.COPY) {
				continue;
			}

			Table table = operation.getTables().iterator().next();

			List<Column> identityColumns = table.getIdentityColumns();
			Set<Column> columns = operation.getColumns().stream()
					.map(table::getColumn)
					.collect(Collectors.toSet());

			if (!columns.containsAll(identityColumns)) {
				Set<Step> dependencies = step.getTransitiveDependencies();

				boolean migratesIdentities = false;
				for (Step dependency : dependencies) {
					Operation dependencyOperation = dependency.getOperation();
					if (dependencyOperation.getType() != Type.COPY) {
						continue;
					}

					Table other = dependencyOperation.getTables().iterator().next();
					Set<Column> dependencyColumns = dependencyOperation.getColumns().stream()
							.map(other::getColumn)
							.collect(Collectors.toSet());

					if (other.equals(table) && dependencyColumns.containsAll(identityColumns)) {
						migratesIdentities = true;
						break;
					}
				}

				checkState(migratesIdentities, "Identities are not migrated first: " + table.getName());
			}
		}
	}

	private static void verifyThatNotNullableForeignKeysAreSatisfiedBeforeInitialCopy(Plan plan) {
		for (Step step : plan.getSteps()) {
			Operation operation = step.getOperation();
			if (operation.getType() != Type.COPY) {
				continue;
			}

			Table table = operation.getTables().iterator().next();

			Set<Table> requiresTables = table.getForeignKeys().stream()
					.filter(ForeignKey::isNotNullable)
					.map(ForeignKey::getReferredTable)
					.distinct()
					.collect(Collectors.toSet());

			for (Table requiresTable : requiresTables) {
				if (!plan.getGhostTables().contains(requiresTable)) {
					continue;
				}

				Set<String> requiredIdentityColumns = table.getForeignKeys().stream()
						.filter(ForeignKey::isNotNullable)
						.filter(fk -> fk.getReferredTable().equals(requiresTable))
						.flatMap(fk -> fk.getReferredColumns().stream())
						.collect(Collectors.toSet());

				Set<Step> dependencies = step.getTransitiveDependencies();
				boolean satisfied = false;
				for (Step dependency : dependencies) {
					Operation dependencyOperation = dependency.getOperation();
					Set<Table> others = dependencyOperation.getTables();
					if (!others.contains(requiresTable)) {
						continue;
					}

					if (dependencyOperation.getType() == Type.ADD_NULL) {
						satisfied = true;
						break;
					}
					else if (dependencyOperation.getType() == Type.COPY) {
						Set<String> dependencyColumns = dependencyOperation.getColumns().stream()
								.map(requiresTable::getColumn)
								.map(Column::getName)
								.collect(Collectors.toSet());

						if (dependencyColumns.containsAll(requiredIdentityColumns)) {
							satisfied = true;
							break;
						}
					}
				}

				checkState(satisfied, "Identities of parent table: " + requiresTable.getName()
						+ " should be migrated before copying records of: " + table.getName());
			}
		}
	}
}
