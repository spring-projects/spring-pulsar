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

package org.springframework.pulsar.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumerSpec;
import org.apache.pulsar.reactive.client.api.ReactiveMessageReaderSpec;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderSpec;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.pulsar.autoconfigure.PulsarReactiveProperties.SchedulerType;

import reactor.core.scheduler.Schedulers;

/**
 * Unit tests for {@link PulsarReactiveProperties}.
 *
 * @author Christophe Bornet
 */
public class PulsarReactivePropertiesTests {

	private final PulsarReactiveProperties properties = new PulsarReactiveProperties();

	private void bind(String name, String value) {
		bind(Collections.singletonMap(name, value));
	}

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
			props.put("spring.pulsar.reactive.sender.message-routing-mode", "custompartition");
			props.put("spring.pulsar.reactive.sender.hashing-scheme", "murmur3_32hash");
			props.put("spring.pulsar.reactive.sender.crypto-failure-action", "send");
			props.put("spring.pulsar.reactive.sender.batching-max-publish-delay", "5s");
			props.put("spring.pulsar.reactive.sender.round-robin-router-batching-partition-switch-frequency", "6");
			props.put("spring.pulsar.reactive.sender.batching-max-messages", "7");
			props.put("spring.pulsar.reactive.sender.batching-max-bytes", "8");
			props.put("spring.pulsar.reactive.sender.batching-enabled", "false");
			props.put("spring.pulsar.reactive.sender.chunking-enabled", "true");
			props.put("spring.pulsar.reactive.sender.encryption-keys[0]", "my-key");
			props.put("spring.pulsar.reactive.sender.compression-type", "lz4");
			props.put("spring.pulsar.reactive.sender.initial-sequence-id", "9");
			props.put("spring.pulsar.reactive.sender.producer-access-mode", "exclusive");
			props.put("spring.pulsar.reactive.sender.lazy-start=partitioned-producers", "true");
			props.put("spring.pulsar.reactive.sender.properties[my-prop]", "my-prop-value");

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
			assertThat(senderSpec.getRoundRobinRouterBatchingPartitionSwitchFrequency()).isEqualTo(6);
			assertThat(senderSpec.getBatchingMaxMessages()).isEqualTo(7);
			assertThat(senderSpec.getBatchingMaxBytes()).isEqualTo(8);
			assertThat(senderSpec.getBatchingEnabled()).isEqualTo(false);
			assertThat(senderSpec.getChunkingEnabled()).isEqualTo(true);
			assertThat(senderSpec.getEncryptionKeys()).containsExactly("my-key");
			assertThat(senderSpec.getCompressionType()).isEqualTo(CompressionType.LZ4);
			assertThat(senderSpec.getInitialSequenceId()).isEqualTo(9);
			assertThat(senderSpec.getAccessMode()).isEqualTo(ProducerAccessMode.Exclusive);
			assertThat(senderSpec.getLazyStartPartitionedProducers()).isTrue();
			assertThat(senderSpec.getProperties()).hasSize(1).containsEntry("my-prop", "my-prop-value");
		}

	}

	@Nested
	class ConsumerPropertiesTests {

		@Test
		void consumerPropsToConsumerSpec() {
			Map<String, String> props = new HashMap<>();
			props.put("spring.pulsar.reactive.consumer.topics[0]", "my-topic");
			props.put("spring.pulsar.reactive.consumer.topics-pattern", "my-pattern");
			props.put("spring.pulsar.reactive.consumer.subscription-name", "my-subscription");
			props.put("spring.pulsar.reactive.consumer.subscription-type", "shared");
			props.put("spring.pulsar.reactive.consumer.subscription-mode", "nondurable");
			props.put("spring.pulsar.reactive.consumer.subscription-properties[my-sub-prop]", "my-sub-prop-value");
			props.put("spring.pulsar.reactive.consumer.receiver-queue-size", "1");
			props.put("spring.pulsar.reactive.consumer.acknowledgements-group-time", "2s");
			props.put("spring.pulsar.reactive.consumer.acknowledge-asynchronously", "false");
			props.put("spring.pulsar.reactive.consumer.negative-ack-redelivery-delay", "3s");
			props.put("spring.pulsar.reactive.consumer.dead-letter-policy.max-redeliver-count", "4");
			props.put("spring.pulsar.reactive.consumer.dead-letter-policy.retry-letter-topic", "my-retry-topic");
			props.put("spring.pulsar.reactive.consumer.dead-letter-policy.dead-letter-topic", "my-dlt-topic");
			props.put("spring.pulsar.reactive.consumer.dead-letter-policy.initial-subscription-name",
					"my-initial-subscription");
			props.put("spring.pulsar.reactive.consumer.max-total-receiver-queue-size-across-partitions", "5");
			props.put("spring.pulsar.reactive.consumer.consumer-name", "my-consumer");
			props.put("spring.pulsar.reactive.consumer.ack-timeout", "6s");
			props.put("spring.pulsar.reactive.consumer.ack-timeout-tick-time", "7s");
			props.put("spring.pulsar.reactive.consumer.priority-level", "8");
			props.put("spring.pulsar.reactive.consumer.crypto-failure-action", "discard");
			props.put("spring.pulsar.reactive.consumer.properties[my-prop]", "my-prop-value");
			props.put("spring.pulsar.reactive.consumer.read-compacted", "true");
			props.put("spring.pulsar.reactive.consumer.batch-index-ack-enabled", "true");
			props.put("spring.pulsar.reactive.consumer.subscription-initial-position", "earliest");
			props.put("spring.pulsar.reactive.consumer.topics-pattern-auto-discovery-period", "9s");
			props.put("spring.pulsar.reactive.consumer.topics-pattern-subscription-mode", "alltopics");
			props.put("spring.pulsar.reactive.consumer.auto-update-partitions", "false");
			props.put("spring.pulsar.reactive.consumer.auto-update-partitions-interval", "10s");
			props.put("spring.pulsar.reactive.consumer.replicate-subscription-state", "true");
			props.put("spring.pulsar.reactive.consumer.auto-ack-oldest-chunked-message-on-queue-full", "false");
			props.put("spring.pulsar.reactive.consumer.max-pending-chunked-message", "11");
			props.put("spring.pulsar.reactive.consumer.expire-time-of-incomplete-chunked-message", "12s");

			bind(props);
			ReactiveMessageConsumerSpec consumerSpec = properties.buildReactiveMessageConsumerSpec();

			assertThat(consumerSpec.getTopicNames()).containsExactly("my-topic");
			assertThat(consumerSpec.getTopicsPattern().toString()).isEqualTo("my-pattern");
			assertThat(consumerSpec.getSubscriptionName()).isEqualTo("my-subscription");
			assertThat(consumerSpec.getSubscriptionType()).isEqualTo(SubscriptionType.Shared);
			assertThat(consumerSpec.getSubscriptionMode()).isEqualTo(SubscriptionMode.NonDurable);
			assertThat(consumerSpec.getSubscriptionProperties()).hasSize(1).containsEntry("my-sub-prop",
					"my-sub-prop-value");
			assertThat(consumerSpec.getReceiverQueueSize()).isEqualTo(1);
			assertThat(consumerSpec.getAcknowledgementsGroupTime()).isEqualTo(Duration.ofSeconds(2));
			assertThat(consumerSpec.getAcknowledgeAsynchronously()).isFalse();
			assertThat(consumerSpec.getNegativeAckRedeliveryDelay()).isEqualTo(Duration.ofSeconds(3));
			assertThat(consumerSpec.getDeadLetterPolicy().getMaxRedeliverCount()).isEqualTo(4);
			assertThat(consumerSpec.getDeadLetterPolicy().getRetryLetterTopic()).isEqualTo("my-retry-topic");
			assertThat(consumerSpec.getDeadLetterPolicy().getDeadLetterTopic()).isEqualTo("my-dlt-topic");
			assertThat(consumerSpec.getDeadLetterPolicy().getInitialSubscriptionName())
					.isEqualTo("my-initial-subscription");
			assertThat(consumerSpec.getMaxTotalReceiverQueueSizeAcrossPartitions()).isEqualTo(5);
			assertThat(consumerSpec.getConsumerName()).isEqualTo("my-consumer");
			assertThat(consumerSpec.getAckTimeout()).isEqualTo(Duration.ofSeconds(6));
			assertThat(consumerSpec.getAckTimeoutTickTime()).isEqualTo(Duration.ofSeconds(7));
			assertThat(consumerSpec.getPriorityLevel()).isEqualTo(8);
			assertThat(consumerSpec.getCryptoFailureAction()).isEqualTo(ConsumerCryptoFailureAction.DISCARD);
			assertThat(consumerSpec.getProperties()).hasSize(1).containsEntry("my-prop", "my-prop-value");
			assertThat(consumerSpec.getReadCompacted()).isTrue();
			assertThat(consumerSpec.getBatchIndexAckEnabled()).isTrue();
			assertThat(consumerSpec.getSubscriptionInitialPosition()).isEqualTo(SubscriptionInitialPosition.Earliest);
			assertThat(consumerSpec.getTopicsPatternAutoDiscoveryPeriod()).isEqualTo(Duration.ofSeconds(9));
			assertThat(consumerSpec.getTopicsPatternSubscriptionMode()).isEqualTo(RegexSubscriptionMode.AllTopics);
			assertThat(consumerSpec.getAutoUpdatePartitions()).isFalse();
			assertThat(consumerSpec.getAutoUpdatePartitionsInterval()).isEqualTo(Duration.ofSeconds(10));
			assertThat(consumerSpec.getReplicateSubscriptionState()).isTrue();
			assertThat(consumerSpec.getAutoAckOldestChunkedMessageOnQueueFull()).isFalse();
			assertThat(consumerSpec.getMaxPendingChunkedMessage()).isEqualTo(11);
			assertThat(consumerSpec.getExpireTimeOfIncompleteChunkedMessage()).isEqualTo(Duration.ofSeconds(12));
		}

		@ParameterizedTest
		@EnumSource(value = SchedulerType.class, names = "immediate", mode = Mode.EXCLUDE)
		void acknowledgeScheduler(SchedulerType acknowledgeSchedulerType) {
			bind("spring.pulsar.reactive.consumer.acknowledge-scheduler-type", acknowledgeSchedulerType.name());
			ReactiveMessageConsumerSpec consumerSpec = properties.buildReactiveMessageConsumerSpec();

			assertThat(consumerSpec.getAcknowledgeScheduler().toString())
					.isEqualTo("Schedulers.%s()".formatted(acknowledgeSchedulerType));
		}

		@Test
		void acknowledgeSchedulerImmediate() {
			bind("spring.pulsar.reactive.consumer.acknowledge-scheduler-type", "immediate");
			ReactiveMessageConsumerSpec consumerSpec = properties.buildReactiveMessageConsumerSpec();

			assertThat(consumerSpec.getAcknowledgeScheduler()).isSameAs(Schedulers.immediate());
		}

	}

	@Nested
	class ReaderPropertiesTests {

		@Test
		void readerPropsToReaderSpec() {
			Map<String, String> props = new HashMap<>();
			props.put("spring.pulsar.reactive.reader.topic-names[0]", "my-topic");
			props.put("spring.pulsar.reactive.reader.reader-name", "my-reader");
			props.put("spring.pulsar.reactive.reader.subscription-name", "my-subscription");
			props.put("spring.pulsar.reactive.reader.generated-subscription-name-prefix", "my-prefix");
			props.put("spring.pulsar.reactive.reader.receiver-queue-size", "1");
			props.put("spring.pulsar.reactive.reader.read-compacted", "true");
			props.put("spring.pulsar.reactive.reader.key-hash-ranges[0].start", "2");
			props.put("spring.pulsar.reactive.reader.key-hash-ranges[0].end", "3");
			props.put("spring.pulsar.reactive.reader.crypto-failure-action", "discard");

			bind(props);
			ReactiveMessageReaderSpec readerSpec = properties.buildReactiveMessageReaderSpec();

			assertThat(readerSpec.getTopicNames()).containsExactly("my-topic");
			assertThat(readerSpec.getReaderName()).isEqualTo("my-reader");
			assertThat(readerSpec.getSubscriptionName()).isEqualTo("my-subscription");
			assertThat(readerSpec.getGeneratedSubscriptionNamePrefix()).isEqualTo("my-prefix");
			assertThat(readerSpec.getReceiverQueueSize()).isEqualTo(1);
			assertThat(readerSpec.getReadCompacted()).isTrue();
			assertThat(readerSpec.getKeyHashRanges()).containsExactly(Range.of(2, 3));
			assertThat(readerSpec.getCryptoFailureAction()).isEqualTo(ConsumerCryptoFailureAction.DISCARD);
		}

	}

}
