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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.pulsar.spring.cloud.stream.binder.properties.PulsarExtendedBindingProperties;

/**
 * Tests for {@link PulsarExtendedBindingProperties}.
 *
 * @author Chris Bono
 */
public class PulsarExtendedBindingPropertiesTests {

	private final PulsarExtendedBindingProperties properties = new PulsarExtendedBindingProperties();

	private void bind(Map<String, String> map) {
		ConfigurationPropertySource source = new MapConfigurationPropertySource(map);
		new Binder(source).bind("spring.cloud.stream.pulsar", Bindable.ofInstance(this.properties));
	}

	@Test
	void producerProperties() {
		// Only spot check a few values (PulsarPropertiesTests does the heavy lifting)
		Map<String, String> props = new HashMap<>();
		props.put("spring.cloud.stream.pulsar.bindings.my-foo.producer.topic-name", "my-topic");
		props.put("spring.cloud.stream.pulsar.bindings.my-foo.producer.send-timeout", "2s");
		props.put("spring.cloud.stream.pulsar.bindings.my-foo.producer.max-pending-messages", "3");
		props.put("spring.cloud.stream.pulsar.bindings.my-foo.producer.producer-access-mode", "exclusive");
		props.put("spring.cloud.stream.pulsar.bindings.my-foo.producer.properties[my-prop]", "my-prop-value");

		bind(props);

		assertThat(properties.getBindings()).containsOnlyKeys("my-foo");
		Map<String, Object> producerProps = properties.getExtendedProducerProperties("my-foo").buildProperties();
		// Verify that the props can be loaded in a ProducerBuilder
		assertThatNoException().isThrownBy(() -> ConfigurationDataUtils.loadData(producerProps,
				new ProducerConfigurationData(), ProducerConfigurationData.class));
		// @formatter:off
		assertThat(producerProps)
				.containsEntry("topicName", "my-topic")
				.containsEntry("sendTimeoutMs", 2_000)
				.containsEntry("maxPendingMessages", 3)
				.containsEntry("accessMode", ProducerAccessMode.Exclusive)
				.hasEntrySatisfying("properties", properties ->
						assertThat(properties)
								.asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class))
								.containsEntry("my-prop", "my-prop-value"));
		// @formatter:on
	}

	@Test
	void consumerProperties() {
		// Only spot check a few values (PulsarPropertiesTests does the heavy lifting)
		Map<String, String> props = new HashMap<>();
		props.put("spring.cloud.stream.pulsar.bindings.my-foo.consumer.topics[0]", "my-topic");
		props.put("spring.cloud.stream.pulsar.bindings.my-foo.consumer.subscription-properties[my-sub-prop]",
				"my-sub-prop-value");
		props.put("spring.cloud.stream.pulsar.bindings.my-foo.consumer.subscription-mode", "nondurable");
		props.put("spring.cloud.stream.pulsar.bindings.my-foo.consumer.receiver-queue-size", "1");

		bind(props);

		assertThat(properties.getBindings()).containsOnlyKeys("my-foo");
		Map<String, Object> consumerProps = properties.getExtendedConsumerProperties("my-foo").buildProperties();
		// Verify that the props can be loaded in a ConsumerBuilder
		assertThatNoException().isThrownBy(() -> ConfigurationDataUtils.loadData(consumerProps,
				new ConsumerConfigurationData<>(), ConsumerConfigurationData.class));
		// @formatter:off
		assertThat(consumerProps)
				.hasEntrySatisfying("topicNames",
						topics -> assertThat(topics).asInstanceOf(InstanceOfAssertFactories.collection(String.class))
								.containsExactly("my-topic"))
				.hasEntrySatisfying("subscriptionProperties",
						properties -> assertThat(properties)
								.asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class))
								.containsEntry("my-sub-prop", "my-sub-prop-value"))
				.containsEntry("subscriptionMode", SubscriptionMode.NonDurable)
				.containsEntry("receiverQueueSize", 1);
		// @formatter:on
	}

}
