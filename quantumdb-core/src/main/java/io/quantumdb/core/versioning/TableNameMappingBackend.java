package io.quantumdb.core.versioning;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Sets;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.versioning.TableNameMappingDataBackend.TableMappingEntry;
import io.quantumdb.core.versioning.TableNameMappingDataBackend.TableMappingKey;

public class TableNameMappingBackend {

	public TableMapping load(Backend backend, Changelog changelog) throws SQLException {
		try (Connection connection = backend.connect()) {
			BackendUtils.ensureQuantumDbTablesExist(backend, connection);
			return BackendUtils.inTransaction(connection, () -> {
				TableNameMappingDataBackend tableNameMappingDataBackend = new TableNameMappingDataBackend();
				Map<TableMappingKey, TableMappingEntry> tableMappingEntries = tableNameMappingDataBackend.load(backend, connection);

				TableMapping tableMapping = new TableMapping();
				for (TableMappingEntry entry : tableMappingEntries.values()) {
					Version version = changelog.getVersion(entry.getVersionId());
					tableMapping.add(version, entry.getTableName(), entry.getTableId());
				}

				return tableMapping;
			});
		}
	}

	public void persist(Backend backend, TableMapping mapping) throws SQLException {
		try (Connection connection = backend.connect()) {
			BackendUtils.ensureQuantumDbTablesExist(backend, connection);
			BackendUtils.inTransaction(connection, () -> {
				TableNameMappingDataBackend tableNameMappingDataBackend = new TableNameMappingDataBackend();
				Map<TableMappingKey, TableMappingEntry> tableMappingEntries = tableNameMappingDataBackend.load(backend,
						connection);

				Set<TableMappingKey> keys = Sets.newHashSet();
				for (Version version : mapping.getVersions()) {
					Map<String, String> internalMapping = mapping.getTableMapping(version);
					for (Entry<String, String> entry : internalMapping.entrySet()) {
						TableMappingKey key = new TableMappingKey(entry.getValue(), version.getId());
						TableMappingEntry tableMappingEntry;
						if (tableMappingEntries.containsKey(key)) {
							tableMappingEntry = tableMappingEntries.get(key);
						}
						else {
							tableMappingEntry = tableNameMappingDataBackend.create(key);
						}
						tableMappingEntry.setTableId(entry.getKey());
						tableMappingEntry.persist();

						keys.add(key);
					}
				}

				for (TableMappingKey key : tableMappingEntries.keySet()) {
					if (!keys.contains(key)) {
						tableNameMappingDataBackend.delete(key);
					}
				}

				return null;
			});
		}
	}

}
