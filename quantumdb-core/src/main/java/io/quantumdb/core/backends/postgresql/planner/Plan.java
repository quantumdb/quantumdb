package io.quantumdb.core.backends.postgresql.planner;


import static com.google.common.base.Preconditions.checkArgument;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.quantumdb.core.backends.postgresql.planner.Operation.Type;
import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.schema.definitions.Table;
import lombok.Data;

@Data
public class Plan {

	static class Builder {

		private final MigrationState state;
		private final List<Step> steps;

		private Builder(MigrationState state) {
			this.state = state;
			this.steps = Lists.newArrayList();
		}

		public ImmutableList<Step> getSteps() {
			return ImmutableList.copyOf(steps);
		}

		public Step copy(Table table, Set<String> columns) {
			checkArgument(!columns.isEmpty(), "You cannot copy 0 columns!");

			Set<String> toDo = state.getYetToBeMigratedColumns(table.getName());
			LinkedHashSet<String> filtered = Sets.newLinkedHashSet(Sets.intersection(columns, toDo));

			if (filtered.isEmpty()) {
				for (int i = steps.size() - 1; i >= 0; i--) {
					Step step = steps.get(i);
					Operation operation = step.getOperation();
					if (operation.getTables().equals(Sets.newHashSet(table)) && operation.getType() == Type.COPY) {
						return step;
					}
				}
			}

			Step step = new Step(new Operation(table, filtered, Type.COPY));
			state.markColumnsAsMigrated(table.getName(), filtered);
			steps.add(step);
			return step;
		}

		public Step addNullRecord(Set<Table> tables) {
			for (Step step : steps) {
				Operation operation = step.getOperation();
				if (operation.getTables().equals(tables) && operation.getType() == Type.ADD_NULL) {
					return step;
				}
			}
			Step step = new Step(new Operation(tables, Type.ADD_NULL));
			steps.add(0, step);
			return step;
		}

		public Step dropNullRecord(Set<Table> tables) {
			for (Step step : steps) {
				Operation operation = step.getOperation();
				if (operation.getTables().equals(tables) && operation.getType() == Type.DROP_NULL) {
					return step;
				}
			}
			Step step = new Step(new Operation(tables, Type.DROP_NULL));
			steps.add(step);
			return step;
		}

		public Optional<Step> findFirstCopy(Table table) {
			return steps.stream()
					.filter(step -> {
						Operation operation = step.getOperation();
						return operation.getTables().contains(table) && operation.getType() == Type.COPY;
					})
					.findFirst();
		}

		public Plan build(DataMappings dataMappings, Set<Table> ghostTables) {
			return new Plan(Lists.newArrayList(steps), dataMappings, ghostTables);
		}

	}

	public static Builder builder(MigrationState migrationState) {
		return new Builder(migrationState);
	}

	private final ImmutableList<Step> steps;
	private final DataMappings dataMappings;
	private final ImmutableSet<Table> ghostTables;

	public Plan(List<Step> steps, DataMappings dataMappings, Set<Table> ghostTables) {
		this.steps = ImmutableList.copyOf(steps);
		this.dataMappings = dataMappings;
		this.ghostTables = ImmutableSet.copyOf(ghostTables);
	}

	public ImmutableList<Step> getSteps() {
		return steps;
	}

	public Optional<Step> nextStep() {
		return steps.stream()
				.filter(Step::canBeExecuted)
				.findFirst();
	}

	public boolean isExecuted() {
		return steps.stream()
				.allMatch(Step::isExecuted);
	}

	@Override
	public String toString() {
		List<Step> toPrint = Lists.newArrayList(steps);

		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < toPrint.size(); i++) {
			Step step = toPrint.get(i);
			Set<Step> dependencies = step.getDependencies();

			builder.append((i + 1) + ".\t");
			builder.append(step);

			if (!dependencies.isEmpty()) {
				builder.append(" depends on: ");
				builder.append(dependencies.stream()
						.map(toPrint::indexOf)
						.map(position -> position + 1)
						.collect(Collectors.toList()));
			}

			builder.append("\n");
		}
		return builder.toString();
	}

}
