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

package org.springframework.pulsar.core.reactive;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.api.MutableReactiveMessageConsumerSpec;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumer;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumerSpec;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DefaultReactivePulsarConsumerFactory}
 *
 * @author Christophe Bornet
 */
class DefaultReactiveMessageConsumerFactoryTests {

	private static final Schema<String> SCHEMA = Schema.STRING;

	@Test
	void createConsumer() {
		MutableReactiveMessageConsumerSpec spec = new MutableReactiveMessageConsumerSpec();
		spec.setConsumerName("test-consumer");
		DefaultReactivePulsarConsumerFactory<String> consumerFactory = new DefaultReactivePulsarConsumerFactory<>(
				AdaptedReactivePulsarClientFactory.create((PulsarClient) null), spec);

		ReactiveMessageConsumer<String> consumer = consumerFactory.createConsumer(SCHEMA);

		assertThat(consumer)
				.extracting("consumerSpec", InstanceOfAssertFactories.type(ReactiveMessageConsumerSpec.class))
				.extracting(ReactiveMessageConsumerSpec::getConsumerName).isEqualTo("test-consumer");
	}

	@Test
	void createConsumerWithCustomizer() {
		MutableReactiveMessageConsumerSpec spec = new MutableReactiveMessageConsumerSpec();
		spec.setConsumerName("test-consumer");
		DefaultReactivePulsarConsumerFactory<String> consumerFactory = new DefaultReactivePulsarConsumerFactory<>(
				AdaptedReactivePulsarClientFactory.create((PulsarClient) null), spec);

		ReactiveMessageConsumer<String> consumer = consumerFactory.createConsumer(SCHEMA,
				Collections.singletonList(builder -> builder.consumerName("new-test-consumer")));

		assertThat(consumer)
				.extracting("consumerSpec", InstanceOfAssertFactories.type(ReactiveMessageConsumerSpec.class))
				.extracting(ReactiveMessageConsumerSpec::getConsumerName).isEqualTo("new-test-consumer");
	}

}
