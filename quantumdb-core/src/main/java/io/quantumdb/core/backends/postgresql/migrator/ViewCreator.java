package io.quantumdb.core.backends.postgresql.migrator;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Maps;
import io.quantumdb.core.schema.definitions.View;
import io.quantumdb.core.utils.QueryBuilder;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.Version;
import io.quantumdb.query.rewriter.PostgresqlQueryRewriter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ViewCreator {

	public void create(Connection connection, Collection<View> views, RefLog refLog, Version version) throws SQLException {
		Map<String, String> mapping = Maps.newHashMap();
		refLog.getViewRefs(version).forEach(ref -> mapping.put(ref.getName(), ref.getRefId()));
		refLog.getTableRefs(version).forEach(ref -> mapping.put(ref.getName(), ref.getRefId()));

		PostgresqlQueryRewriter rewriter = new PostgresqlQueryRewriter();
		rewriter.setTableMapping(mapping);

		for (View view : views) {
			create(connection, view, rewriter);
		}
	}

	private void create(Connection connection, View view, PostgresqlQueryRewriter rewriter) throws SQLException {
		log.info("Creating view: {}", view.getName());

		QueryBuilder queryBuilder = new QueryBuilder();
		queryBuilder.append("CREATE ");

		if (view.isTemporary()) {
			queryBuilder.append("TEMPORARY ");
		}

		if (view.isRecursive()) {
			queryBuilder.append("RECURSIVE ");
		}

		String rewrittenQuery = rewriter.rewrite(view.getQuery());
		System.out.println(rewrittenQuery);

		queryBuilder.append("VIEW " + view.getName() + " AS (" + rewrittenQuery + ")");
		execute(connection, queryBuilder);
	}

	private void execute(Connection connection, QueryBuilder queryBuilder) throws SQLException {
		String query = queryBuilder.toString();
		try (Statement statement = connection.createStatement()) {
			log.debug("Executing: " + query);
			statement.execute(query);
		}
	}

}
