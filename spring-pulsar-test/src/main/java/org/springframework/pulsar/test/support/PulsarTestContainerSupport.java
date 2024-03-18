/*
 * Copyright 2022-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.pulsar.test.support;

import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Provides a static {@link PulsarContainer} that can be shared across test classes.
 *
 * @author Chris Bono
 * @author Kartik Shrivastava
 */
@Testcontainers(disabledWithoutDocker = true)
public interface PulsarTestContainerSupport {

	PulsarContainer PULSAR_CONTAINER = new PulsarContainer(getPulsarImage());

	static DockerImageName getPulsarImage() {
		return DockerImageName.parse("apachepulsar/pulsar:latest");
	}

	@BeforeAll
	static void startContainer() {
		PULSAR_CONTAINER.start();
	}

	static void stopContainer2() {
		PULSAR_CONTAINER.stop();
	}

	static String getPulsarBrokerUrl() {
		return PULSAR_CONTAINER.getPulsarBrokerUrl();
	}

	static String getHttpServiceUrl() {
		return PULSAR_CONTAINER.getHttpServiceUrl();
	}

	static boolean isContainerStarted() {
		return PULSAR_CONTAINER.isRunning();
	}

}
