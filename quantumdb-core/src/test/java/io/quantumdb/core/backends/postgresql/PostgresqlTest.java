package io.quantumdb.core.backends.postgresql;

import static org.junit.Assume.assumeTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;

import io.quantumdb.core.utils.RandomHasher;
import lombok.Getter;
import org.junit.After;
import org.junit.Before;

@Getter
public abstract class PostgresqlTest {

	private Connection connection;
	private String catalogName;
	private String jdbcUrl;
	private String jdbcUser;
	private String jdbcPass;

	@Before
	public void setUp() throws SQLException, ClassNotFoundException {
		this.jdbcUrl = getProperty("jdbc.url").orElse("jdbc:postgresql://localhost:5432");
		this.jdbcUser = getProperty("jdbc.user", "PG_USER").orElse(null);
		this.jdbcPass = getProperty("jdbc.pass", "PG_PASSWORD").orElse(null);

		assumeTrue("No 'jdbc.user' or 'PG_USER' specified", jdbcUser != null);
		assumeTrue("No 'jdbc.pass' or 'PG_PASSWORD' specified", jdbcPass != null);

		this.catalogName = "db_" + RandomHasher.generateHash();
		try (Connection conn = DriverManager.getConnection(jdbcUrl + "/" + jdbcUser, jdbcUser, jdbcPass)) {
			conn.createStatement().execute("DROP DATABASE IF EXISTS " + catalogName + ";");
			conn.createStatement().execute("CREATE DATABASE " + catalogName + ";");
		}

		this.connection = DriverManager.getConnection(jdbcUrl + "/" + catalogName, jdbcUser, jdbcPass);
	}

	@After
	public void tearDown() throws SQLException {
		connection.close();
		try (Connection conn = DriverManager.getConnection(jdbcUrl + "/" + jdbcUser, jdbcUser, jdbcPass)) {
			conn.createStatement().execute("SELECT COUNT(pg_terminate_backend(pg_stat_activity.pid))"
					+ "FROM pg_stat_activity "
					+ "WHERE pg_stat_activity.datname = '" + catalogName + "' "
					+ "AND usename = current_user "
					+ "AND pid <> pg_backend_pid();");

			conn.createStatement().execute("DROP DATABASE " + catalogName + ";");
		}
	}

	public Optional<String> getProperty(String... keys) {
		for (String key : keys) {
			String property = System.getProperty(key);
			if (property != null) {
				return Optional.of(property);
			}
			property = System.getenv(key);
			if (property != null) {
				return Optional.of(property);
			}
		}
		return Optional.empty();
	}

}
