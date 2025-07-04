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

package org.springframework.pulsar.inttest.function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.amqp.autoconfigure.RabbitAutoConfiguration;
import org.springframework.boot.pulsar.autoconfigure.PulsarAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternUtils;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.function.PulsarFunctionAdministration;
import org.springframework.pulsar.function.PulsarFunctionOperations.FunctionStopPolicy;
import org.springframework.pulsar.function.PulsarSource;
import org.springframework.pulsar.inttest.function.PulsarFunctionAdministrationIntegrationTests.ContainerLoggingTestWatcher;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;

/**
 * Integration tests for {@link PulsarFunctionAdministration}.
 * <p>
 * Sets up a Rabbit container and a Rabbit source and verifies end-end functionality.
 *
 * @author Chris Bono
 */
@Testcontainers(disabledWithoutDocker = true)
@EnabledIf("rabbitConnectorExists")
@ExtendWith(ContainerLoggingTestWatcher.class)
class PulsarFunctionAdministrationIntegrationTests {

	private static final String RABBIT_QUEUE = "pft_foo_queue";

	private static final String PULSAR_TOPIC = "pft_foo-topic";

	private static final PulsarContainer PULSAR_CONTAINER = new PulsarContainer(
			PulsarTestContainerSupport.getPulsarImage());

	private static final RabbitMQContainer RABBITMQ_CONTAINER = new RabbitMQContainer("rabbitmq");

	@BeforeAll
	static void startContainers() {
		Network sharedNetwork = Network.newNetwork();
		// @formatter:off
		PULSAR_CONTAINER
				.withNetwork(sharedNetwork)
				.withFunctionsWorker()
				.withClasspathResourceMapping("/connectors/", "/pulsar/connectors", BindMode.READ_ONLY)
				.start();
		RABBITMQ_CONTAINER
				.withNetwork(sharedNetwork)
				.withNetworkAliases("rabbitmq")
				.withExposedPorts(5672, 15672)
				.withStartupTimeout(Duration.ofMinutes(1))
				.start();
		// @formatter:on
	}

	private static final CountDownLatch RECEIVED_MESSAGE_LATCH = new CountDownLatch(10);

	private static final List<String> RECEIVED_MESSAGES = new ArrayList<>();

	static void messageReceived(String message) {
		RECEIVED_MESSAGE_LATCH.countDown();
		RECEIVED_MESSAGES.add(message);
	}

	@Test
	void verifyRabbitSourceIsCreatedAndMessagesAreSourcedIntoPulsar() throws Exception {
		SpringApplication app = new SpringApplication(PulsarFunctionTestConfiguration.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		try (ConfigurableApplicationContext context = app.run(
				"--spring.pulsar.client.service-url=" + PULSAR_CONTAINER.getPulsarBrokerUrl(),
				"--spring.pulsar.admin.service-url=" + PULSAR_CONTAINER.getHttpServiceUrl(),
				"--spring.rabbitmq.host=" + RABBITMQ_CONTAINER.getHost(),
				"--spring.rabbitmq.port=" + RABBITMQ_CONTAINER.getAmqpPort())) {

			// Give source time to get ready
			Thread.sleep(20000);

			// Send messages to rabbit and wait for them to come through the rabbit source
			RabbitTemplate rabbitTemplate = context.getBean(RabbitTemplate.class);
			List<String> messages = LongStream.range(0, RECEIVED_MESSAGE_LATCH.getCount())
				.mapToObj((i) -> "bar" + i)
				.toList();
			messages.forEach(msg -> rabbitTemplate.convertAndSend(RABBIT_QUEUE, msg));

			assertThat(RECEIVED_MESSAGE_LATCH.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(RECEIVED_MESSAGES).containsExactlyElementsOf(messages);
		}
	}

	@Test
	void verifyStopPolicyIsEnforcedOnShutdown() throws Exception {
		SpringApplication app = new SpringApplication(PulsarFunctionStopPolicyTestConfiguration.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		try (ConfigurableApplicationContext ignored = app.run(
				"--spring.pulsar.client.service-url=" + PULSAR_CONTAINER.getPulsarBrokerUrl(),
				"--spring.pulsar.admin.service-url=" + PULSAR_CONTAINER.getHttpServiceUrl(),
				"--spring.rabbitmq.host=" + RABBITMQ_CONTAINER.getHost(),
				"--spring.rabbitmq.port=" + RABBITMQ_CONTAINER.getAmqpPort())) {

			// Give source time to get ready
			Thread.sleep(20000);

			// Verify the sources are up and running
			try (PulsarAdmin admin = getAdmin()) {
				assertSourceExistsWithStatus("rabbit-test-source-none", true, admin);
				assertSourceExistsWithStatus("rabbit-test-source-stop", true, admin);
				assertSourceExistsWithStatus("rabbit-test-source-delete", true, admin);
			}
		}

		// Give app context time to close and run the stop policies
		Thread.sleep(10000);

		// Stop policy runs after context close - verify source are in expected state
		try (PulsarAdmin admin = getAdmin()) {
			assertSourceExistsWithStatus("rabbit-test-source-none", true, admin);
			assertSourceExistsWithStatus("rabbit-test-source-stop", false, admin);
			assertSourceDoesNotExist("rabbit-test-source-delete", admin);
		}
	}

	private PulsarAdmin getAdmin() throws PulsarClientException {
		return PulsarAdmin.builder().serviceHttpUrl(PULSAR_CONTAINER.getHttpServiceUrl()).build();
	}

	private void assertSourceExistsWithStatus(String name, boolean isRunning, PulsarAdmin admin)
			throws PulsarAdminException {
		assertThat(admin.sources().getSourceStatus("public", "default", name)).isNotNull()
			.extracting(SourceStatus::getNumRunning)
			.isEqualTo(isRunning ? 1 : 0);
	}

	private void assertSourceDoesNotExist(String name, PulsarAdmin admin) {
		assertThatThrownBy(() -> admin.sources().getSourceStatus("public", "default", name))
			.isInstanceOf(NotFoundException.class);
	}

	static boolean rabbitConnectorExists() {
		try {
			Resource[] connectors = ResourcePatternUtils.getResourcePatternResolver(new DefaultResourceLoader())
				.getResources("classpath:/connectors/**");
			boolean available = Arrays.stream(connectors)
				.map(Resource::getFilename)
				.filter(Objects::nonNull)
				.anyMatch((name) -> name.contains("pulsar-io-rabbitmq"));
			if (!available) {
				logTestDisabledReason();
				return false;
			}
			return true;
		}
		catch (IOException e) {
			logTestDisabledReason();
			return false;
		}
	}

	private static void logTestDisabledReason() {
		System.err.printf("Skipping %s - Rabbit connector was not available in 'src/test/resources/connectors/'%n",
				PulsarFunctionAdministrationIntegrationTests.class.getName());
	}

	static PulsarSource rabbitPulsarSource(@Nullable FunctionStopPolicy stopPolicy) {
		// This Rabbit host/port config is what the Pulsar container uses to contact
		// the Rabbit container. So that container-container is reachable we use a
		// custom network and a network alias 'rabbitmq' and the exposed port '5672'.
		// This differs from typical RabbitTemplate/RabbitProperties coordinates which
		// require the mapped host and port (outside the container).
		String suffix = stopPolicy != null ? ("-" + stopPolicy.name().toLowerCase()) : "";
		Map<String, Object> configs = new HashMap<>();
		configs.put("host", "rabbitmq");
		configs.put("port", 5672);
		configs.put("virtualHost", "/");
		configs.put("username", "guest");
		configs.put("password", "guest");
		configs.put("queueName", RABBIT_QUEUE + suffix);
		configs.put("connectionName", "pft_foo_connection" + suffix);
		SourceConfig sourceConfig = SourceConfig.builder()
			.tenant("public")
			.namespace("default")
			.name("rabbit-test-source" + suffix)
			.archive("builtin://rabbitmq")
			.topicName(PULSAR_TOPIC + suffix)
			.configs(configs)
			.build();
		return new PulsarSource(sourceConfig, stopPolicy != null ? stopPolicy : FunctionStopPolicy.DELETE, null);
	}

	@Configuration(proxyBeanMethods = false)
	@Import({ PulsarAutoConfiguration.class, RabbitAutoConfiguration.class })
	static class PulsarFunctionTestConfiguration {

		@Bean
		PulsarSource rabbitSource() {
			return PulsarFunctionAdministrationIntegrationTests.rabbitPulsarSource(null);
		}

		@PulsarListener(topics = PULSAR_TOPIC, subscriptionName = "pft-foo-sub")
		public void listen(String msg) {
			PulsarFunctionAdministrationIntegrationTests.messageReceived(msg);
		}

	}

	@Configuration(proxyBeanMethods = false)
	@Import(PulsarAutoConfiguration.class)
	static class PulsarFunctionStopPolicyTestConfiguration {

		@Bean
		PulsarSource rabbitSourceWithStopPolicyNone() {
			return PulsarFunctionAdministrationIntegrationTests.rabbitPulsarSource(FunctionStopPolicy.NONE);
		}

		@Bean
		PulsarSource rabbitSourceWithStopPolicyStop() {
			return PulsarFunctionAdministrationIntegrationTests.rabbitPulsarSource(FunctionStopPolicy.STOP);
		}

		@Bean
		PulsarSource rabbitSourceWithStopPolicyDelete() {
			return PulsarFunctionAdministrationIntegrationTests.rabbitPulsarSource(FunctionStopPolicy.DELETE);
		}

	}

	static class ContainerLoggingTestWatcher implements TestWatcher {

		private final LogAccessor logger = new LogAccessor(this.getClass());

		@Override
		public void testFailed(ExtensionContext context, Throwable cause) {
			this.logger.error(() -> "Test %s failed due to: %s - inspect container logs below:%n%n%s"
				.formatted(context.getDisplayName(), cause.getMessage(), getPulsarContainerLogs()));
		}

		private String getPulsarContainerLogs() {
			try {
				return PULSAR_CONTAINER.getLogs();
			}
			catch (Exception ex) {
				String msg = "<---- Failed to retrieve container logs: %s ---->".formatted(ex.getMessage());
				this.logger.error(ex, msg);
				return msg;
			}
		}

	}

}
