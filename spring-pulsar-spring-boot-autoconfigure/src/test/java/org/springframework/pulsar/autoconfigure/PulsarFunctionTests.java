/*
 * Copyright 2023-2023 the original author or authors.
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

package org.springframework.pulsar.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.io.SourceConfig;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.pulsar.function.PulsarFunctionAdministration;
import org.springframework.pulsar.function.PulsarFunctionAdministration.PulsarFunctionException;
import org.springframework.pulsar.function.PulsarFunctionOperations;
import org.springframework.pulsar.function.PulsarFunctionOperations.FunctionStopPolicy;
import org.springframework.pulsar.function.PulsarSource;

/**
 * Integration tests for {@link PulsarFunctionAdministration}.
 *
 * <p>
 * Verifies end-to-end that a user-configured {@link PulsarSource} results in a call to
 * the Pulsar broker to register the source connector.
 *
 * @author Chris Bono
 */
class PulsarFunctionTests implements PulsarTestContainerSupport {

	// Not currently used by anything but prepares the test for when we do verify end-end
	@Container
	static RabbitMQContainer rabbit = new RabbitMQContainer("rabbitmq").withExposedPorts(5672, 15672)
			.withStartupTimeout(Duration.ofMinutes(1));

	/**
	 * Because the docker image we use does not contain the built-in connectors we only
	 * verify that the configured functions are attempted to be registered w/ the broker.
	 *
	 * <p>
	 * Later we may provide a docker image that contains the built-in connectors and
	 * verify the complete end-end flow.
	 */
	@Test
	void verifyPulsarSourceIsAttemptedToBeCreatedOnBroker() {
		SpringApplication app = new SpringApplication(PulsarFunctionTestConfiguration.class);
		app.setWebApplicationType(WebApplicationType.NONE);

		// Again, this is a temp solution to verification of this feature
		PulsarFunctionException thrown = catchThrowableOfType(
				() -> app.run("--spring.pulsar.client.serviceUrl=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
						"--spring.pulsar.administration.service-url=" + PulsarTestContainerSupport.getHttpServiceUrl(),
						"--spring.rabbitmq.host=" + rabbit.getHost(), "--spring.rabbitmq.port=" + rabbit.getAmqpPort()),
				PulsarFunctionException.class);

		Map<PulsarFunctionOperations<?>, Exception> failures = thrown.getFailures();
		assertThat(failures).hasSize(1);
		Map.Entry<PulsarFunctionOperations<?>, Exception> failureEntry = failures.entrySet().iterator().next();
		assertThat(failureEntry.getKey()).isInstanceOf(PulsarSource.class)
				.extracting("config", InstanceOfAssertFactories.type(SourceConfig.class))
				.extracting(SourceConfig::getName).isEqualTo("rabbit-test-source");
		assertThat(failureEntry.getValue()).isInstanceOf(PulsarAdminException.class)
				.hasMessageContaining("Built-in source is not available");
	}

	@Configuration(proxyBeanMethods = false)
	@Import(PulsarAutoConfiguration.class)
	static class PulsarFunctionTestConfiguration {

		@Bean
		PulsarSource rabbitSource(@Value("${spring.rabbitmq.host}") String rabbitHost,
				@Value("${spring.rabbitmq.port}") int rabbitPort) {
			Map<String, Object> configs = new HashMap<>();
			configs.put("host", rabbitHost);
			configs.put("port", rabbitPort);
			configs.put("virtualHost", "/");
			configs.put("username", "guest");
			configs.put("password", "guest");
			configs.put("queueName", "test_rabbit");
			configs.put("connectionName", "test-connection");
			SourceConfig sourceConfig = SourceConfig.builder().tenant("public").namespace("default")
					.name("rabbit-test-source").archive("builtin://rabbitmq").topicName("incoming_rabbit")
					.configs(configs).build();
			return new PulsarSource(sourceConfig, FunctionStopPolicy.NONE, null);
		}

	}

}
