package io.quantumdb.core.backends.postgresql.planner;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Table;
import lombok.Data;

class Graph {

	@Data
	static class GraphResult {
		private final long count;
		private final Set<String> tableNames;
	}

	public static Graph fromCatalog(Catalog catalog, Set<String> tableNames) {
		Graph graph = new Graph(catalog);
		for (String tableName : tableNames) {
			graph.nodes.put(tableName, new TableNode(tableName, Lists.newArrayList()));
		}

		for (String tableName : tableNames) {
			TableNode node = graph.nodes.get(tableName);

			Table table = catalog.getTable(tableName);
			table.getForeignKeys().forEach(foreignKey -> {
				String referredTableName = foreignKey.getReferredTableName();
				if (tableNames.contains(referredTableName)) {
					node.getForeignKeys().add(foreignKey);
				}
			});
		}

		return graph;
	}

	private final Catalog catalog;
	private final Map<String, TableNode> nodes;

	public Graph(Catalog catalog) {
		this.catalog = catalog;
		this.nodes = Maps.newHashMap();
	}

	public Set<String> getTableIds() {
		return nodes.keySet();
	}

	public boolean isEmpty() {
		return nodes.isEmpty();
	}

	public TableNode get(String tableName) {
		return nodes.get(tableName);
	}

	public void remove(String tableName) {
		TableNode removed = nodes.remove(tableName);
		if (removed != null) {
			nodes.values().forEach(node -> {
				Set<ForeignKey> toRemoved = Sets.newHashSet();
				node.getForeignKeys().forEach(foreignKey -> {
					if (tableName.equals(foreignKey.getReferredTableName())) {
						toRemoved.add(foreignKey);
					}
				});

				toRemoved.forEach(node.getForeignKeys()::remove);
			});
		}
	}

	public Optional<GraphResult> leastOutgoingForeignKeys(Set<String> tableIds) {
		Map<String, Long> outgoingForeignKeys = nodes.entrySet().stream()
				.filter(entry -> tableIds.contains(entry.getKey()))
				.collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().getForeignKeys().stream()
						.filter(foreignKey -> !foreignKey.isSelfReferencing())
						.filter(foreignKey -> tableIds.contains(foreignKey.getReferredTableName()))
						.map(ForeignKey::getReferredTable)
						.distinct()
						.count()));

		if (outgoingForeignKeys.isEmpty()) {
			return Optional.of(new GraphResult(0, tableIds));
		}

		long minimum = outgoingForeignKeys.values().stream()
				.reduce(Long.MAX_VALUE, Math::min);

		Set<String> tableNames = outgoingForeignKeys.entrySet().stream()
				.filter(entry -> entry.getValue() == minimum)
				.map(Entry::getKey)
				.collect(Collectors.toSet());

		return Optional.of(new GraphResult(minimum, tableNames));
	}

	public Optional<GraphResult> mostIncomingForeignKeys(Set<String> tableIds) {
		Map<String, Long> incomingForeignKeys = nodes.entrySet().stream()
				.filter(entry -> tableIds.contains(entry.getKey()))
				.collect(Collectors.toMap(Entry::getKey, entry -> {
					String tableName = entry.getKey();
					return catalog.getForeignKeys().stream()
							.filter(foreignKey -> foreignKey.getReferredTableName().equals(tableName))
							.filter(ForeignKey::isNotNullable)
							.map(ForeignKey::getReferencingTableName)
							.distinct()
							.count();
				}));

		if (incomingForeignKeys.isEmpty()) {
			return Optional.of(new GraphResult(0, tableIds));
		}

		long maximum = incomingForeignKeys.values().stream()
				.reduce(0L, Math::max);

		Set<String> tableNames = incomingForeignKeys.entrySet().stream()
				.filter(entry -> entry.getValue() == maximum)
				.map(Entry::getKey)
				.collect(Collectors.toSet());

		return Optional.of(new GraphResult(maximum, tableNames));
	}

}
