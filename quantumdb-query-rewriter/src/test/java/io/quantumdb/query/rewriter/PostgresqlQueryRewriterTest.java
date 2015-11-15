package io.quantumdb.query.rewriter;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class PostgresqlQueryRewriterTest {

	private Map<String, String> tableMappings;
	private PostgresqlQueryRewriter rewriter;

	@Before
	public void setUp() {
		this.rewriter = new PostgresqlQueryRewriter();
		this.tableMappings = new HashMap<>();

		tableMappings.put("accounts", "accounts_v2");
		tableMappings.put("users", "users_v2");
		tableMappings.put("transactions", "transactions");
	}

	@Test
	public void testSimpleSelectQuery() throws SQLException {
		String input = "SELECT * FROM users";
		String expected = "SELECT * FROM users_v2";
		assertEquals(expected, rewrite(input));
	}

	@Test
	public void testSimpleSelectQueryWithSchema() throws SQLException {
		String input = "SELECT * FROM public.users";
		String expected = "SELECT * FROM public.users_v2";
		assertEquals(expected, rewrite(input));
	}

	@Test
	public void testThatUnquotedTableNameInFromClauseIsCaseInsensitive() throws SQLException {
		String input = "SELECT * FROM Users";
		String expected = "SELECT * FROM users_v2";
		assertEquals(expected, rewrite(input));
	}

	@Test
	public void testSelectQueryWithTableNameBetweenQuotes() throws SQLException {
		String input = "SELECT * FROM 'users'";
		String expected = "SELECT * FROM 'users_v2'";
		assertEquals(expected, rewrite(input));
	}

	@Ignore
	@Test(expected = SQLException.class)
	public void testThatQuotedTableNameInFromIsCaseSensitive() throws SQLException {
		String input = "SELECT * FROM 'Users'";
		rewrite(input);
	}

	@Test
	public void testSelectWithShortHandAlias() throws SQLException {
		String input = "SELECT * FROM users users WHERE users.id = 1;";
		String expected = "SELECT * FROM users_v2 users WHERE users.id = 1;";
		assertEquals(expected, rewrite(input));
	}

	@Test
	public void testSelectWithAlias() throws SQLException {
		String input = "SELECT * FROM users AS users WHERE users.id = 1;";
		String expected = "SELECT * FROM users_v2 AS users WHERE users.id = 1;";
		assertEquals(expected, rewrite(input));
	}

	@Test
	@Ignore
	public void testImplicitJoin() throws SQLException {
		String input = "SELECT * FROM users, accounts WHERE users.id = accounts.owner_id";
		String expected = "SELECT * FROM users_v2, accounts_v2 WHERE users_v2.id = accounts_v2.owner_id";
		assertEquals(expected, rewrite(input));
	}

	@Test
	@Ignore
	public void testImplicitJoinWithShortHandAliases() throws SQLException {
		String input = "SELECT * FROM users users, accounts accounts WHERE users.id = accounts.owner_id";
		String expected = "SELECT * FROM users_v2 users, accounts_v2 accounts WHERE users.id = accounts.owner_id";
		assertEquals(expected, rewrite(input));
	}

	@Test
	@Ignore
	public void testImplicitJoinWithAliases() throws SQLException {
		String input = "SELECT * FROM users AS users, accounts AS accounts WHERE users.id = accounts.owner_id";
		String expected = "SELECT * FROM users_v2 AS users, accounts_v2 AS accounts WHERE users.id = accounts.owner_id";
		assertEquals(expected, rewrite(input));
	}

	private String rewrite(String query) throws SQLException {
		rewriter.setTableMapping(tableMappings);
		return rewriter.rewrite(query);
	}

}
