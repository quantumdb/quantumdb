package io.quantumdb.core.backends.guice;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class PersistenceConfiguration {

	private static final String JDBC_URL_KEY = "javax.persistence.jdbc.url";

	public static Properties load() throws IOException {
		Properties properties = new Properties();
		try (InputStream stream = PersistenceConfiguration.class.getResourceAsStream("/persistence.properties")) {
			checkNotNull(stream, "Persistence properties not found");
			properties.load(stream);
		}

		if (!isNullOrEmpty(System.getProperty(JDBC_URL_KEY))) {
			String url = System.getProperty(JDBC_URL_KEY);
			properties.setProperty(JDBC_URL_KEY, url);
			log.warn("Overriding {} with {}", JDBC_URL_KEY, url);
		}

		return properties;
	}

}
