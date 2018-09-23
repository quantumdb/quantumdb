package io.quantumdb.driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import io.quantumdb.query.rewriter.PostgresqlQueryRewriter;
import io.quantumdb.query.rewriter.QueryRewriter;

public class Driver implements java.sql.Driver {

	static {
		try {
			java.sql.DriverManager.registerDriver(new Driver());
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private java.sql.Driver delegate;
	private Transformer transformer;

	private Driver() {
		// Prevent others from instantiating this class.
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		if (!url.startsWith("jdbc:quantumdb:")) {
			return null;
		}
		url = url.substring(0, 4) + ":" + url.substring(15, url.length());

		try {
			Class.forName("org.postgresql.Driver");
		}
		catch (ClassNotFoundException e) {
			throw new SQLException("Could not locate delegate driver.", e);
		}

		String version = parseVersion(url);
		if (version != null) {
			String applicationName = info.getProperty("ApplicationName");
			if (applicationName != null && !applicationName.equals("")) {
				applicationName += " - " + version;
			}
			else {
				applicationName = "QuantumDB driver - " + version;
			}
			info.setProperty("ApplicationName", applicationName);
		}

		this.delegate = DriverManager.getDriver(url);
		Connection connection = delegate.connect(url, info);
		QueryRewriter queryRewriter = new PostgresqlQueryRewriter();
		this.transformer = new Transformer(connection, queryRewriter, version);

		return new ProxyConnection(connection, transformer);
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return url != null && url.startsWith("jdbc:quantumdb:");
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		return delegate.getPropertyInfo(url, info);
	}

	@Override
	public int getMajorVersion() {
		return delegate.getMajorVersion();
	}

	@Override
	public int getMinorVersion() {
		return delegate.getMinorVersion();
	}

	@Override
	public boolean jdbcCompliant() {
		return delegate.jdbcCompliant();
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return delegate.getParentLogger();
	}

	public String getRefId(String tableName) {
		return transformer.getRefId(tableName);
	}

	private String parseVersion(String url) {
		String[] lookups = new String[] { "?version=", "&version=" };

		int index = -1;
		for (String lookup : lookups) {
			index = url.indexOf(lookup);
			if (index > -1) {
				index += lookup.length();
				break;
			}
		}

		if (index == -1) {
			return null;
		}

		String chunk = url.substring(index);
		int end = chunk.indexOf("&");
		if (end == -1) {
			end = chunk.length();
		}

		return chunk.substring(0, end);
	}

}
