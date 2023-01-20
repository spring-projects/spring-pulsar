/*
 * Copyright 2023 the original author or authors.
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

package org.springframework.pulsar.spring.cloud.stream.binder;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;

@ExtendWith(OutputCaptureExtension.class)
class PulsarBinderIntegrationTests implements PulsarTestContainerSupport {

	@Test
	void basicProducerConsumerBindingEndToEnd(CapturedOutput output) {
		SpringApplication app = new SpringApplication(BasicScenarioConfig.class);
		app.setWebApplicationType(WebApplicationType.NONE);
		try (ConfigurableApplicationContext context = app.run(
				"--spring.pulsar.client.service-url=" + PulsarTestContainerSupport.getPulsarBrokerUrl(),
				"--spring.cloud.function.definition=textSupplier;textLogger",
				"--spring.cloud.stream.bindings.textLogger-in-0.destination=textSupplier-out-0",
				"--spring.cloud.stream.pulsar.bindings.textLogger-in-0.consumer.subscription-name=basic-scenario-sub-1")) {
			Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> output.toString().contains("Hello binder: test-basic-scenario"));
		}
	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	static class BasicScenarioConfig {

		private final Logger logger = LoggerFactory.getLogger(BasicScenarioConfig.class);

		@Bean
		public Supplier<String> textSupplier() {
			return () -> "test-basic-scenario";
		}

		@Bean
		public Consumer<String> textLogger() {
			return s -> this.logger.info("Hello binder: " + s);
		}

	}

}
