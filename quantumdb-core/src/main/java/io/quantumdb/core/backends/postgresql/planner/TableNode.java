package io.quantumdb.core.backends.postgresql.planner;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import io.quantumdb.core.schema.definitions.ForeignKey;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(of = { "tableName" })
class TableNode {
	private final String tableName;
	private final List<ForeignKey> foreignKeys;

	@Override
	public String toString() {
		if (foreignKeys.isEmpty()) {
			return tableName;
		}

		Set<String> referencedTables = foreignKeys.stream()
				.map(ForeignKey::getReferredTableName)
				.collect(Collectors.toSet());

		return tableName + ": [ " + Joiner.on(", ").join(referencedTables) + " ]";
	}
}
