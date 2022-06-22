package org.springframework.pulsar.core;

import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

abstract class AbstractContainerBaseTest {

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

