package io.quantumdb.core.migration;

import static io.quantumdb.core.schema.operations.SchemaOperations.addColumn;

import java.sql.SQLException;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.quantumdb.core.backends.guice.PersistenceModule;

public class Test {

	public static void main(String[] args) throws SQLException, InterruptedException {
		Injector injector = Guice.createInjector(new PersistenceModule());
		Migrator migrator = injector.getInstance(Migrator.class);
		migrator.addSchemaState("9652b7b856", "482a3a6284");
	}

//	public static void main(String[] args) throws SQLException {
//		Injector injector = Guice.createInjector(new PersistenceModule());
//		Backend backend = injector.getInstance(Backend.class);
//
//		State state = backend.loadState();
//		Changelog changelog = state.getChangelog();
//
//		// Register pre-existing tables in root version.
//		Catalog catalog = state.getCatalog();
//		TableMapping mapping = state.getTableMapping();
//		for (Table table : catalog.getTables()) {
//			mapping.set(changelog.getRoot(), table.getName(), table.getName());
//		}
//
//		// Add schema change.
//		changelog.addChangeSet("Michael de Jong",
//				addColumn("students__networks", "activated", bool(), "false", NOT_NULL));
//
//		backend.persistState(state);
//
//		System.out.println("Root version: " + changelog.getRoot());
//		System.out.println("Latest version: " + changelog.getLastAdded());
//	}

}
