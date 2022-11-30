/*
 * Copyright 2022 the original author or authors.
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

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.pulsar.autoconfigure.SpringPulsarBootAppSanityTests.SpringPulsarBootTestApp;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Sanity tests to ensure that {@code Spring Pulsar} can be auto-configured into a Spring
 * Boot application.
 *
 * @author Chris Bono
 */
@SpringBootTest(classes = SpringPulsarBootTestApp.class, webEnvironment = WebEnvironment.RANDOM_PORT)
@Disabled("temporarily")
class SpringPulsarBootAppSanityTests implements PulsarTestContainerSupport {

	@DynamicPropertySource
	static void pulsarProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.pulsar.client.service-url", PulsarTestContainerSupport::getPulsarBrokerUrl);
	}

	@Test
	void appStartsWithAutoConfiguredSpringPulsarComponents(
			@Autowired ObjectProvider<PulsarTemplate<String>> pulsarTemplate) {
		assertThat(pulsarTemplate.getIfAvailable()).isNotNull();
	}

	@Test
	void templateCanBeAccessedDuringWebRequest(@Autowired TestRestTemplate restTemplate) {
		String body = restTemplate.getForObject("/hello", String.class);
		assertThat(body).startsWith("Hello World -> ");
	}

	@SpringBootConfiguration
	@EnableAutoConfiguration
	static class SpringPulsarBootTestApp {

		@Autowired
		private ObjectProvider<PulsarTemplate<String>> pulsarTemplateProvider;

		@RestController
		class TestWebController {

			@GetMapping("/hello")
			String sayHello() throws PulsarClientException {

				PulsarTemplate<String> pulsarTemplate = pulsarTemplateProvider.getIfAvailable();
				if (pulsarTemplate == null) {
					return "NOPE! Not hello world";
				}
				MessageId msgId = pulsarTemplate.send("spbast-hello-topic", "hello");
				return "Hello World -> " + msgId;
			}

		}

	}

}
