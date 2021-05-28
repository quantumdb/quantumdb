package io.quantumdb.core.backends;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Config {

	private static final List<String> SUPPORTED_BACKENDS = Lists.newArrayList(
			"io.quantumdb.core.planner.PostgresqlBackend");

	private static final String URL = "url";
	private static final String USER = "user";
	private static final String CATALOG = "catalog";
	private static final String PASSWORD = "password";
	private static final String DRIVER = "driver";
	private static final String DRY_RUN = "dryRun";
	private static final String OUTPUT_FILE = "outputFile";

	private static final String FILE = ".quantumdb";

	public static Config load() throws IOException {
		Properties properties = new Properties();
		File file = new File(FILE);
		if (file.exists()) {
			properties.load(new FileReader(file));
		}
		return new Config(properties);
	}

	private final Properties persistentProperties;
	private final Properties transientProperties;

	public Config() {
		this(new Properties());
	}

	private Config(Properties persistentProperties) {
		this(persistentProperties, new Properties());
	}

	private Config(Properties persistentProperties, Properties transientProperties) {
		this.persistentProperties = persistentProperties;
		this.transientProperties = transientProperties;
	}

	public String getUrl() {
		return persistentProperties.getProperty(URL);
	}

	public Config setUrl(String url) {
		persistentProperties.setProperty(URL, url);
		return this;
	}

	public String getUser() {
		return persistentProperties.getProperty(USER);
	}

	public Config setUser(String user) {
		persistentProperties.setProperty(USER, user);
		return this;
	}

	public String getCatalog() {
		return persistentProperties.getProperty(CATALOG);
	}

	public Config setCatalog(String catalog) {
		persistentProperties.setProperty(CATALOG, catalog);
		return this;
	}

	public String getPassword() {
		return persistentProperties.getProperty(PASSWORD);
	}

	public Config setPassword(String password) {
		persistentProperties.setProperty(PASSWORD, password);
		return this;
	}

	public String getDriver() {
		return persistentProperties.getProperty(DRIVER);
	}

	public Config setDriver(String password) {
		persistentProperties.setProperty(DRIVER, password);
		return this;
	}

	public Config enableDryRun(String outputFile) {
		transientProperties.setProperty(DRY_RUN, Boolean.toString(true));
		transientProperties.setProperty(OUTPUT_FILE, outputFile);
		return this;
	}

	public Config disableDryRun() {
		transientProperties.setProperty(DRY_RUN, Boolean.toString(false));
		transientProperties.remove(OUTPUT_FILE);
		return this;
	}

	public boolean isDryRun() {
		return Boolean.toString(true).equalsIgnoreCase(transientProperties.getProperty(DRY_RUN));
	}

	public String getOutputFile() {
		return transientProperties.getProperty(OUTPUT_FILE);
	}

	public void persist() throws IOException {
		try (FileWriter fileWriter = new FileWriter(FILE)) {
			persistentProperties.store(fileWriter, null);
		}
	}

	public Backend getBackend() {
		String jdbcUrl = getUrl();
		checkArgument(jdbcUrl != null, "You have not specified a backend URL.");

		for (String backendName : SUPPORTED_BACKENDS) {
			try {
				Class<?> type = Class.forName(backendName);
				Backend backend = (Backend) type.getDeclaredConstructor(Config.class).newInstance(this);
				if (backend.isJdbcUrlSupported(jdbcUrl)) {
					return backend;
				}
			}
			catch (ReflectiveOperationException e) {
				throw new IllegalArgumentException("Something went wrong selecting backends.", e);
				// Skip this one.
			}
		}

		throw new IllegalArgumentException("No backend support for JDBC URL: " + jdbcUrl);
	}

}
