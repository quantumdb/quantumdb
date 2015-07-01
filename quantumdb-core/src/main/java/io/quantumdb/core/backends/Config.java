package io.quantumdb.core.backends;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import io.quantumdb.core.backends.postgresql.PostgresqlBackend;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Config {

	private static final String URL = "url";
	private static final String USER = "user";
	private static final String CATALOG = "catalog";
	private static final String PASSWORD = "password";
	private static final String DRIVER = "driver";

	private static final String FILE = ".quantumdb";

	public static Config load() throws IOException {
		Properties properties = new Properties();
		File file = new File(FILE);
		if (file.exists()) {
			properties.load(new FileReader(file));
		}
		return new Config(properties);
	}

	private final Properties properties;

	public Config() {
		this(new Properties());
	}

	private Config(Properties properties) {
		this.properties = properties;
	}

	public String getUrl() {
		return properties.getProperty(URL);
	}

	public Config setUrl(String url) {
		properties.setProperty(URL, url);
		return this;
	}

	public String getUser() {
		return properties.getProperty(USER);
	}

	public Config setUser(String user) {
		properties.setProperty(USER, user);
		return this;
	}

	public String getCatalog() {
		return properties.getProperty(CATALOG);
	}

	public Config setCatalog(String catalog) {
		properties.setProperty(CATALOG, catalog);
		return this;
	}

	public String getPassword() {
		return properties.getProperty(PASSWORD);
	}

	public Config setPassword(String password) {
		properties.setProperty(PASSWORD, password);
		return this;
	}

	public String getDriver() {
		return properties.getProperty(DRIVER);
	}

	public Config setDriver(String password) {
		properties.setProperty(DRIVER, password);
		return this;
	}

	public void persist() throws IOException {
		try (FileWriter fileWriter = new FileWriter(FILE)) {
			properties.store(fileWriter, null);
		}
	}

	public Backend getBackend() {
		String driver = getDriver();
		switch (driver) {
			case "org.postgresql.Driver":
				return new PostgresqlBackend(getUrl(), getUser(), getPassword(), getCatalog(), driver);
			default:
				throw new IllegalArgumentException("No backend support for driver: " + driver);
		}
	}

}
