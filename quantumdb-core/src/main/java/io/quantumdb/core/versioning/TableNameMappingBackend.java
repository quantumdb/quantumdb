package io.quantumdb.core.versioning;

import static io.quantumdb.core.versioning.QRawVersion.rawVersion;
import static io.quantumdb.core.versioning.QTableNameMapping.tableNameMapping;

import javax.inject.Provider;
import javax.persistence.EntityManager;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.RowSortedTable;
import com.google.common.collect.TreeBasedTable;
import com.google.inject.Inject;
import com.google.inject.persist.Transactional;
import com.mysema.query.jpa.impl.JPAQuery;

public class TableNameMappingBackend {

	private final Provider<EntityManager> entityManagerProvider;

	@Inject
	TableNameMappingBackend(Provider<EntityManager> entityManagerProvider) {
		this.entityManagerProvider = entityManagerProvider;
	}

	@Transactional
	public TableMapping load(Changelog changelog) {
		TableMapping tableMapping = new TableMapping();

		EntityManager entityManager = entityManagerProvider.get();
		new JPAQuery(entityManager).from(tableNameMapping)
				.list(tableNameMapping)
				.forEach(mapping -> {
					String tableName = mapping.getTableName();
					String versionId = mapping.getVersion().getId();
					String tableId = mapping.getTableId();

					Version version = changelog.getVersion(versionId);
					tableMapping.set(version, tableName, tableId);
				});

		return tableMapping;
	}

	@Transactional
	public void persist(Changelog changelog, TableMapping tableMapping) {
		EntityManager entityManager = entityManagerProvider.get();
		Map<String, RawVersion> versions = new JPAQuery(entityManager).from(rawVersion)
				.list(rawVersion).stream()
				.collect(Collectors.toMap(RawVersion::getId, Function.identity()));

		RowSortedTable<Version, String, TableNameMapping> versionMapping = TreeBasedTable.create();

		new JPAQuery(entityManager).from(tableNameMapping)
				.list(tableNameMapping).stream()
				.forEach(mapping -> {
					String versionId = mapping.getVersion().getId();
					String tableName = mapping.getTableName();
					Version version = changelog.getVersion(versionId);

					versionMapping.put(version, tableName, mapping);
				});

		List<TableNameMapping> toPersist = Lists.newArrayList();
		List<TableNameMapping> toUpdate = Lists.newArrayList();
		List<TableNameMapping> toDelete = Lists.newArrayList();

		for (Version version : tableMapping.getVersions()) {
			for (String tableName : tableMapping.getTableNames(version)) {
				String tableId = tableMapping.getTableId(version, tableName);

				if (versionMapping.contains(version, tableName)) {
					TableNameMapping mapping = versionMapping.remove(version, tableName);
					if (!mapping.getTableId().equals(tableId)) {
						mapping.setTableId(tableId);
						toUpdate.add(mapping);
					}
				}
				else {
					TableNameMapping mapping = new TableNameMapping();
					mapping.setVersion(versions.get(version.getId()));
					mapping.setTableName(tableName);
					mapping.setTableId(tableId);
					toPersist.add(mapping);
				}
			}
		}

		versionMapping.rowKeySet().forEach(version -> {
			versionMapping.row(version).forEach((tableName, mapping) -> {
				toDelete.add(mapping);
			});
		});

		toDelete.forEach(entityManager::remove);
		toUpdate.forEach(entityManager::merge);
		toPersist.forEach(entityManager::persist);
	}

}
