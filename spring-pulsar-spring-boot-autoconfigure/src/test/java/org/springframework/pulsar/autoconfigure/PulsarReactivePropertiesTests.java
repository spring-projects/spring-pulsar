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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderSpec;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

/**
 * Unit tests for {@link PulsarReactiveProperties}.
 *
 * @author Christophe Bornet
 */
public class PulsarReactivePropertiesTests {

	private final PulsarReactiveProperties properties = new PulsarReactiveProperties();

	private void bind(Map<String, String> map) {
		ConfigurationPropertySource source = new MapConfigurationPropertySource(map);
		new Binder(source).bind("spring.pulsar.reactive", Bindable.ofInstance(this.properties));
	}

	@Nested
	class SenderPropertiesTests {

		@Test
		void senderPropsToSenderSpec() {
			Map<String, String> props = new HashMap<>();
			props.put("spring.pulsar.reactive.sender.topic-name", "my-topic");
			props.put("spring.pulsar.reactive.sender.producer-name", "my-producer");
			props.put("spring.pulsar.reactive.sender.send-timeout", "2s");
			props.put("spring.pulsar.reactive.sender.max-pending-messages", "3");
			props.put("spring.pulsar.reactive.sender.max-pending-messages-across-partitions", "4");
			props.put("spring.pulsar.reactive.sender.message-routing-mode", "CustomPartition");
			props.put("spring.pulsar.reactive.sender.hashing-scheme", "Murmur3_32Hash");
			props.put("spring.pulsar.reactive.sender.crypto-failure-action", "SEND");
			props.put("spring.pulsar.reactive.sender.batching-max-publish-delay", "5s");
			props.put("spring.pulsar.reactive.sender.batching-max-messages", "6");
			props.put("spring.pulsar.reactive.sender.batching-enabled", "false");
			props.put("spring.pulsar.reactive.sender.chunking-enabled", "true");
			props.put("spring.pulsar.reactive.sender.compression-type", "LZ4");
			props.put("spring.pulsar.reactive.sender.producer-access-mode", "Exclusive");

			bind(props);
			ReactiveMessageSenderSpec senderSpec = properties.buildReactiveMessageSenderSpec();

			assertThat(senderSpec.getTopicName()).isEqualTo("my-topic");
			assertThat(senderSpec.getProducerName()).isEqualTo("my-producer");
			assertThat(senderSpec.getSendTimeout()).isEqualTo(Duration.ofSeconds(2));
			assertThat(senderSpec.getMaxPendingMessages()).isEqualTo(3);
			assertThat(senderSpec.getMaxPendingMessagesAcrossPartitions()).isEqualTo(4);
			assertThat(senderSpec.getMessageRoutingMode()).isEqualTo(MessageRoutingMode.CustomPartition);
			assertThat(senderSpec.getHashingScheme()).isEqualTo(HashingScheme.Murmur3_32Hash);
			assertThat(senderSpec.getCryptoFailureAction()).isEqualTo(ProducerCryptoFailureAction.SEND);
			assertThat(senderSpec.getBatchingMaxPublishDelay()).isEqualTo(Duration.ofSeconds(5));
			assertThat(senderSpec.getBatchingMaxMessages()).isEqualTo(6);
			assertThat(senderSpec.getBatchingEnabled()).isEqualTo(false);
			assertThat(senderSpec.getChunkingEnabled()).isEqualTo(true);
			assertThat(senderSpec.getCompressionType()).isEqualTo(CompressionType.LZ4);
			assertThat(senderSpec.getAccessMode()).isEqualTo(ProducerAccessMode.Exclusive);
		}

	}

}
