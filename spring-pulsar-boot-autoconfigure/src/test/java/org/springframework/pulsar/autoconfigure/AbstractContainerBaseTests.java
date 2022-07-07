package org.springframework.pulsar.autoconfigure;

import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

public class AbstractContainerBaseTests {

	static final DockerImageName PULSAR_IMAGE = DockerImageName.parse("apachepulsar/pulsar:2.10.0");

	static PulsarContainer PULSAR_CONTAINER;

	static {
		PULSAR_CONTAINER = new PulsarContainer(PULSAR_IMAGE);
		PULSAR_CONTAINER.start();
	}

	protected static String getPulsarBrokerUrl() {
		return PULSAR_CONTAINER.getPulsarBrokerUrl();
	}

	protected static String getHttpServiceUrl() {
		return PULSAR_CONTAINER.getHttpServiceUrl();
	}

}
