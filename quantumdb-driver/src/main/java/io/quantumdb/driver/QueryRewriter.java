package io.quantumdb.driver;

import java.sql.SQLException;
import java.util.Map;

public interface QueryRewriter {

	/**
	 * Instructs the QueryRewriter to adopt a new set of table mappings which should be used to rewrite queries.
	 *
	 * @param newTableMapping The new tableMapping to adopt.
	 */
	void setTableMapping(Map<String, String> newTableMapping);

	/**
	 * Rewrites a specified query. The resulting String should be used to when querying the database.
	 *
	 * @param query The query to rewrite.
	 * @return The rewritten query.
	 * @throws SQLException In case the specified query could not be rewritten.
	 * @throws IllegalArgumentException When the specified 'query' is NULL.
	 */
	String rewrite(String query) throws SQLException;

}
