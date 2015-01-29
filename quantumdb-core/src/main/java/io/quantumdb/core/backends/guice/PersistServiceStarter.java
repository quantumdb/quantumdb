package io.quantumdb.core.backends.guice;

import com.google.inject.Inject;
import com.google.inject.persist.PersistService;

class PersistServiceStarter {

	@Inject
	PersistServiceStarter(PersistService service) {
		service.start();

		Runtime.getRuntime().addShutdownHook(new Thread(service::stop));
	}

}
