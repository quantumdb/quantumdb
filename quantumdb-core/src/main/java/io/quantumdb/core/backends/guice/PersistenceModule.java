package io.quantumdb.core.backends.guice;

import java.io.IOException;
import java.util.Properties;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.postgresql.PostgresqlBackend;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class PersistenceModule extends AbstractModule {

	private final String url;
	private final String catalog;
	private final String user;
	private final String pass;

	public PersistenceModule() {
		this(null, null, null, null);
	}

	public PersistenceModule(String url, String catalog, String user, String pass) {
		this.url = url + "/" + catalog;
		this.catalog = catalog;
		this.user = user;
		this.pass = pass;
	}

	@Override
	protected void configure() {
		Properties properties = loadProperties();
		if (url != null) {
			properties.setProperty("javax.persistence.jdbc.url", url);
		}
		if (user != null) {
			properties.setProperty("javax.persistence.jdbc.user", user);
		}
		if (pass != null) {
			properties.setProperty("javax.persistence.jdbc.password", pass);
		}
		if (catalog != null) {
			properties.setProperty("javax.persistence.jdbc.catalog", catalog);
		}

		Names.bindProperties(binder(), properties);

		bind(Backend.class).to(selectBackend(properties.getProperty("javax.persistence.jdbc.driver")));
	}

	private Properties loadProperties() {
		try {
			return PersistenceConfiguration.load();
		}
		catch (IOException e) {
			log.error(e.getMessage(), e);
			throw new RuntimeException(e);
		}
	}

	private Class<? extends Backend> selectBackend(String driver) {
		switch (driver) {
			case "org.postgresql.Driver":
				return PostgresqlBackend.class;
			default:
				throw new IllegalArgumentException("No backend support for driver: " + driver);
		}
	}

}
