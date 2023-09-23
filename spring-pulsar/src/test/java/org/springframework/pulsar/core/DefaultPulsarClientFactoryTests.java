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

package org.springframework.pulsar.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatRuntimeException;

import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.Test;

import org.springframework.mock.env.MockEnvironment;

/**
 * Tests for {@link DefaultPulsarClientFactory}.
 *
 * @author Chris Bono
 */
class DefaultPulsarClientFactoryTests {

	@Test
	void constructWithServiceUrl() throws PulsarClientException {
		var clientFactory = new DefaultPulsarClientFactory("pulsar://localhost:5150");
		assertThat(clientFactory.createClient()).hasFieldOrPropertyWithValue("conf.serviceUrl",
				"pulsar://localhost:5150");
	}

	@Test
	void constructWithCustomizer() throws PulsarClientException {
		var clientFactory = new DefaultPulsarClientFactory(
				(clientBuilder) -> clientBuilder.serviceUrl("pulsar://localhost:5150"));
		assertThat(clientFactory.createClient()).hasFieldOrPropertyWithValue("conf.serviceUrl",
				"pulsar://localhost:5150");
	}

	@Test
	void constructWithNullCustomizer() {
		assertThatIllegalArgumentException()
			.isThrownBy(() -> new DefaultPulsarClientFactory((PulsarClientBuilderCustomizer) null))
			.withMessage("customizer must not be null");
	}

	@Test
	void customizerThrowsException() {
		var clientFactory = new DefaultPulsarClientFactory((clientBuilder) -> {
			throw new RuntimeException("Who turned out the lights?");
		});
		assertThatRuntimeException().isThrownBy(clientFactory::createClient).withMessage("Who turned out the lights?");
	}

	@Test
	void createsRestartableClientByDefault() throws PulsarClientException {
		var clientFactory = new DefaultPulsarClientFactory("pulsar://localhost:5150");
		clientFactory.setEnvironment(new MockEnvironment());
		assertThat(clientFactory.createClient()).isInstanceOf(PulsarClientProxy.class);
	}

	@Test
	void createsRestartableClientWhenPropertySetTrue() throws PulsarClientException {
		var clientFactory = new DefaultPulsarClientFactory("pulsar://localhost:5150");
		var env = new MockEnvironment().withProperty("spring.pulsar.client.restartable", "true");
		clientFactory.setEnvironment(env);
		assertThat(clientFactory.createClient()).isInstanceOf(PulsarClientProxy.class);
	}

	@Test
	void createsDefaultClientWhenPropertySetFalse() throws PulsarClientException {
		var clientFactory = new DefaultPulsarClientFactory("pulsar://localhost:5150");
		var env = new MockEnvironment().withProperty("spring.pulsar.client.restartable", "false");
		clientFactory.setEnvironment(env);
		assertThat(clientFactory.createClient()).isNotInstanceOf(PulsarClientProxy.class);
	}

}
