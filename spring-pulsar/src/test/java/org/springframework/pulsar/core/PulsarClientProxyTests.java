/*
 * Copyright 2023-present the original author or authors.
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Tests for {@link PulsarClientProxy}.
 *
 * @author Chris Bono
 */
class PulsarClientProxyTests implements PulsarTestContainerSupport {

	@Test
	void constructWithCustomizer() throws Exception {
		var restartableClient = new PulsarClientProxy(
				(clientBuilder) -> clientBuilder.serviceUrl("pulsar://localhost:5150"));
		restartableClient.afterPropertiesSet();
		assertThat(restartableClient.getInstance()).hasFieldOrPropertyWithValue("conf.serviceUrl",
				"pulsar://localhost:5150");
	}

	@Test
	void constructWithNullCustomizer() {
		assertThatIllegalArgumentException().isThrownBy(() -> new PulsarClientProxy(null))
			.withMessage("customizer must not be null");
	}

	@Test
	void restartLifecycle() throws Exception {
		var serviceUrl = PulsarTestContainerSupport.getPulsarBrokerUrl();
		var restartableClient = new PulsarClientProxy((builder) -> builder.serviceUrl(serviceUrl));
		restartableClient.afterPropertiesSet();
		var delegateClient = restartableClient.getInstance();
		assertThat(delegateClient).isNotNull();
		assertThat(delegateClient).hasFieldOrPropertyWithValue("conf.serviceUrl", serviceUrl);
		assertThat(delegateClient.isClosed()).isFalse();
		assertThat(restartableClient.isClosed()).isFalse();

		// Stop and verify the client is closed
		restartableClient.stop();
		assertThat(restartableClient.isClosed()).isTrue();
		assertThat(delegateClient.isClosed()).isTrue();
		assertThat(restartableClient.getInstance()).isSameAs(delegateClient);

		// Restart and verify the client is created again during start
		restartableClient.start();
		var newDelegateClient = restartableClient.getInstance();
		assertThat(newDelegateClient).isNotNull();
		assertThat(newDelegateClient).hasFieldOrPropertyWithValue("conf.serviceUrl", serviceUrl);
		assertThat(newDelegateClient.isClosed()).isFalse();
		assertThat(restartableClient.isClosed()).isFalse();
		assertThat(newDelegateClient).isNotSameAs(delegateClient);

		// Destroy and verify the client is destroyed as well
		restartableClient.destroy();
		assertThat(newDelegateClient.isClosed()).isTrue();
		assertThat(restartableClient.getInstance()).isNull();
	}

	@Nested
	class DelegateClientTests {

		private PulsarClientProxy restartableClient;

		private PulsarClient delegateClient;

		@BeforeEach
		void createClient() throws Exception {
			var serviceUrl = PulsarTestContainerSupport.getPulsarBrokerUrl();
			restartableClient = new PulsarClientProxy((builder) -> builder.serviceUrl(serviceUrl));
			restartableClient.afterPropertiesSet();
			delegateClient = mock(PulsarClient.class);
			ReflectionTestUtils.setField(restartableClient, "instance", delegateClient);
			assertThat(restartableClient.getInstance()).isSameAs(delegateClient);
		}

		@Test
		void newProducer() {
			this.restartableClient.newProducer();
			verify(this.delegateClient).newProducer();
		}

		@Test
		void newProducerWithSchema() {
			this.restartableClient.newProducer(Schema.STRING);
			verify(this.delegateClient).newProducer(Schema.STRING);
		}

		@Test
		void newConsumer() {
			this.restartableClient.newConsumer();
			verify(this.delegateClient).newConsumer();
		}

		@Test
		void newConsumerWithSchema() {
			this.restartableClient.newConsumer(Schema.STRING);
			verify(this.delegateClient).newConsumer(Schema.STRING);
		}

		@Test
		void newReader() {
			this.restartableClient.newReader();
			verify(this.delegateClient).newReader();
		}

		@Test
		void newReaderWithSchema() {
			this.restartableClient.newReader(Schema.STRING);
			verify(this.delegateClient).newReader(Schema.STRING);
		}

		@SuppressWarnings("deprecation")
		@Test
		void newTableViewBuilder() {
			this.restartableClient.newTableViewBuilder(Schema.STRING);
			verify(this.delegateClient).newTableViewBuilder(Schema.STRING);
		}

		@Test
		void newTableView() {
			this.restartableClient.newTableView();
			verify(this.delegateClient).newTableView();
		}

		@Test
		void newTableViewWithSchema() {
			this.restartableClient.newTableView(Schema.STRING);
			verify(this.delegateClient).newTableView(Schema.STRING);
		}

		@Test
		void updateServiceUrl() throws PulsarClientException {
			this.restartableClient.updateServiceUrl("pulsar://foo:6150");
			verify(this.delegateClient).updateServiceUrl("pulsar://foo:6150");
		}

		@Test
		void getPartitionsForTopic() {
			this.restartableClient.getPartitionsForTopic("zTopic", true);
			verify(this.delegateClient).getPartitionsForTopic("zTopic", true);
		}

		@Test
		@SuppressWarnings({ "deprecation", "removal" })
		void getPartitionsForTopicDeprecated() {
			this.restartableClient.getPartitionsForTopic("zTopic");
			verify(this.delegateClient).getPartitionsForTopic("zTopic");
		}

		@Test
		void close() throws PulsarClientException {
			this.restartableClient.close();
			verify(this.delegateClient).close();
		}

		@Test
		void closeAsync() {
			this.restartableClient.closeAsync();
			verify(this.delegateClient).closeAsync();
		}

		@Test
		void shutdown() throws PulsarClientException {
			this.restartableClient.shutdown();
			verify(this.delegateClient).shutdown();
		}

		@Test
		void isClosed() {
			this.restartableClient.isClosed();
			verify(this.delegateClient).isClosed();
		}

		@Test
		void newTransaction() {
			this.restartableClient.newTransaction();
			verify(this.delegateClient).newTransaction();
		}

	}

}
