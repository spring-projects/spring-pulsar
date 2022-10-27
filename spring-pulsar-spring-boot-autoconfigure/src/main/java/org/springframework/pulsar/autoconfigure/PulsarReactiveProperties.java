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

import java.time.Duration;
import java.util.List;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.reactive.client.api.MutableReactiveMessageReaderSpec;
import org.apache.pulsar.reactive.client.api.MutableReactiveMessageSenderSpec;
import org.apache.pulsar.reactive.client.api.ReactiveMessageReaderSpec;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderSpec;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.PropertyMapper;

/**
 * Configuration properties for Spring for the Apache Pulsar reactive client.
 * <p>
 * Users should refer to Pulsar reactive client documentation for complete descriptions of
 * these properties.
 *
 * @author Christophe Bornet
 */
@ConfigurationProperties(prefix = "spring.pulsar.reactive")
public class PulsarReactiveProperties {

	private final Sender sender = new Sender();

	private final Reader reader = new Reader();

	public Sender getSender() {
		return this.sender;
	}

	public Reader getReader() {
		return this.reader;
	}

	public ReactiveMessageSenderSpec buildReactiveMessageSenderSpec() {
		return this.sender.buildReactiveMessageSenderSpec();
	}

	public ReactiveMessageReaderSpec buildReactiveMessageReaderSpec() {
		return this.reader.buildReactiveMessageReaderSpec();
	}

	public static class Sender {

		/**
		 * Topic the producer will publish to.
		 */
		private String topicName;

		/**
		 * Name for the producer. If not assigned, a unique name is generated.
		 */
		private String producerName;

		/**
		 * Time before a message has to be acknowledged by the broker in milliseconds.
		 */
		private Duration sendTimeout = Duration.ofSeconds(30);

		/**
		 * Maximum number of pending messages for the producer.
		 */
		private Integer maxPendingMessages = 1000;

		/**
		 * Maximum number of pending messages across all the partitions.
		 */
		private Integer maxPendingMessagesAcrossPartitions = 50000;

		/**
		 * Message routing mode for a partitioned producer.
		 */
		private MessageRoutingMode messageRoutingMode = MessageRoutingMode.RoundRobinPartition;

		/**
		 * Message hashing scheme to choose the partition to which the message is
		 * published.
		 */
		private HashingScheme hashingScheme = HashingScheme.JavaStringHash;

		/**
		 * Action the producer will take in case of encryption failure.
		 */
		private ProducerCryptoFailureAction cryptoFailureAction = ProducerCryptoFailureAction.FAIL;

		/**
		 * Time period within which the messages sent will be batched in milliseconds.
		 */
		private Duration batchingMaxPublishDelay = Duration.ofMillis(1);

		/**
		 * Maximum number of messages to be batched.
		 */
		private Integer batchingMaxMessages = 1000;

		/**
		 * Whether to automatically batch messages.
		 */
		private Boolean batchingEnabled = true;

		/**
		 * Whether to split large-size messages into multiple chunks.
		 */
		private Boolean chunkingEnabled = false;

		/**
		 * Message compression type.
		 */
		private CompressionType compressionType;

		/**
		 * Name of the initial subscription of the topic.
		 */
		private String initialSubscriptionName;

		/**
		 * Type of access to the topic the producer requires.
		 */
		private ProducerAccessMode producerAccessMode = ProducerAccessMode.Shared;

		private final Cache cache = new Cache();

		public String getTopicName() {
			return this.topicName;
		}

		public void setTopicName(String topicName) {
			this.topicName = topicName;
		}

		public String getProducerName() {
			return this.producerName;
		}

		public void setProducerName(String producerName) {
			this.producerName = producerName;
		}

		public Duration getSendTimeout() {
			return this.sendTimeout;
		}

		public void setSendTimeout(Duration sendTimeout) {
			this.sendTimeout = sendTimeout;
		}

		public Integer getMaxPendingMessages() {
			return this.maxPendingMessages;
		}

		public void setMaxPendingMessages(Integer maxPendingMessages) {
			this.maxPendingMessages = maxPendingMessages;
		}

		public Integer getMaxPendingMessagesAcrossPartitions() {
			return this.maxPendingMessagesAcrossPartitions;
		}

		public void setMaxPendingMessagesAcrossPartitions(Integer maxPendingMessagesAcrossPartitions) {
			this.maxPendingMessagesAcrossPartitions = maxPendingMessagesAcrossPartitions;
		}

		public MessageRoutingMode getMessageRoutingMode() {
			return this.messageRoutingMode;
		}

		public void setMessageRoutingMode(MessageRoutingMode messageRoutingMode) {
			this.messageRoutingMode = messageRoutingMode;
		}

		public HashingScheme getHashingScheme() {
			return this.hashingScheme;
		}

		public void setHashingScheme(HashingScheme hashingScheme) {
			this.hashingScheme = hashingScheme;
		}

		public ProducerCryptoFailureAction getCryptoFailureAction() {
			return this.cryptoFailureAction;
		}

		public void setCryptoFailureAction(ProducerCryptoFailureAction cryptoFailureAction) {
			this.cryptoFailureAction = cryptoFailureAction;
		}

		public Duration getBatchingMaxPublishDelay() {
			return this.batchingMaxPublishDelay;
		}

		public void setBatchingMaxPublishDelay(Duration batchingMaxPublishDelay) {
			this.batchingMaxPublishDelay = batchingMaxPublishDelay;
		}

		public Integer getBatchingMaxMessages() {
			return this.batchingMaxMessages;
		}

		public void setBatchingMaxMessages(Integer batchingMaxMessages) {
			this.batchingMaxMessages = batchingMaxMessages;
		}

		public Boolean getBatchingEnabled() {
			return this.batchingEnabled;
		}

		public void setBatchingEnabled(Boolean batchingEnabled) {
			this.batchingEnabled = batchingEnabled;
		}

		public Boolean getChunkingEnabled() {
			return this.chunkingEnabled;
		}

		public void setChunkingEnabled(Boolean chunkingEnabled) {
			this.chunkingEnabled = chunkingEnabled;
		}

		public CompressionType getCompressionType() {
			return this.compressionType;
		}

		public void setCompressionType(CompressionType compressionType) {
			this.compressionType = compressionType;
		}

		public String getInitialSubscriptionName() {
			return this.initialSubscriptionName;
		}

		public void setInitialSubscriptionName(String initialSubscriptionName) {
			this.initialSubscriptionName = initialSubscriptionName;
		}

		public ProducerAccessMode getProducerAccessMode() {
			return this.producerAccessMode;
		}

		public void setProducerAccessMode(ProducerAccessMode producerAccessMode) {
			this.producerAccessMode = producerAccessMode;
		}

		public Cache getCache() {
			return this.cache;
		}

		public ReactiveMessageSenderSpec buildReactiveMessageSenderSpec() {
			PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();

			MutableReactiveMessageSenderSpec spec = new MutableReactiveMessageSenderSpec();

			map.from(this::getTopicName).to(spec::setTopicName);
			map.from(this::getProducerName).to(spec::setProducerName);
			map.from(this::getSendTimeout).to(spec::setSendTimeout);
			map.from(this::getMaxPendingMessages).to(spec::setMaxPendingMessages);
			map.from(this::getMaxPendingMessagesAcrossPartitions).to(spec::setMaxPendingMessagesAcrossPartitions);
			map.from(this::getMessageRoutingMode).to(spec::setMessageRoutingMode);
			map.from(this::getHashingScheme).to(spec::setHashingScheme);
			map.from(this::getCryptoFailureAction).to(spec::setCryptoFailureAction);
			map.from(this::getBatchingMaxPublishDelay).to(spec::setBatchingMaxPublishDelay);
			map.from(this::getBatchingMaxMessages).to(spec::setBatchingMaxMessages);
			map.from(this::getBatchingEnabled).to(spec::setBatchingEnabled);
			map.from(this::getChunkingEnabled).to(spec::setChunkingEnabled);
			map.from(this::getCompressionType).to(spec::setCompressionType);
			map.from(this::getInitialSubscriptionName).to(spec::setInitialSubscriptionName);
			map.from(this::getProducerAccessMode).to(spec::setAccessMode);

			return spec;
		}

	}

	public static class Reader {

		private String[] topicNames;

		private String readerName;

		private String subscriptionName;

		private String generatedSubscriptionNamePrefix;

		private Integer receiverQueueSize;

		private Boolean readCompacted;

		private Range[] keyHashRanges;

		private ConsumerCryptoFailureAction cryptoFailureAction;

		public String[] getTopicNames() {
			return this.topicNames;
		}

		public void setTopicNames(String[] topicNames) {
			this.topicNames = topicNames;
		}

		public String getReaderName() {
			return this.readerName;
		}

		public void setReaderName(String readerName) {
			this.readerName = readerName;
		}

		public String getSubscriptionName() {
			return this.subscriptionName;
		}

		public void setSubscriptionName(String subscriptionName) {
			this.subscriptionName = subscriptionName;
		}

		public String getGeneratedSubscriptionNamePrefix() {
			return this.generatedSubscriptionNamePrefix;
		}

		public void setGeneratedSubscriptionNamePrefix(String generatedSubscriptionNamePrefix) {
			this.generatedSubscriptionNamePrefix = generatedSubscriptionNamePrefix;
		}

		public Integer getReceiverQueueSize() {
			return this.receiverQueueSize;
		}

		public void setReceiverQueueSize(Integer receiverQueueSize) {
			this.receiverQueueSize = receiverQueueSize;
		}

		public Boolean getReadCompacted() {
			return this.readCompacted;
		}

		public void setReadCompacted(Boolean readCompacted) {
			this.readCompacted = readCompacted;
		}

		public Range[] getKeyHashRanges() {
			return this.keyHashRanges;
		}

		public void setKeyHashRanges(Range[] keyHashRanges) {
			this.keyHashRanges = keyHashRanges;
		}

		public ConsumerCryptoFailureAction getCryptoFailureAction() {
			return this.cryptoFailureAction;
		}

		public void setCryptoFailureAction(ConsumerCryptoFailureAction cryptoFailureAction) {
			this.cryptoFailureAction = cryptoFailureAction;
		}

		public ReactiveMessageReaderSpec buildReactiveMessageReaderSpec() {
			PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();

			MutableReactiveMessageReaderSpec spec = new MutableReactiveMessageReaderSpec();

			map.from(this::getTopicNames).as(List::of).to(spec::setTopicNames);
			map.from(this::getReaderName).to(spec::setReaderName);
			map.from(this::getSubscriptionName).to(spec::setSubscriptionName);
			map.from(this::getGeneratedSubscriptionNamePrefix).to(spec::setGeneratedSubscriptionNamePrefix);
			map.from(this::getReceiverQueueSize).to(spec::setReceiverQueueSize);
			map.from(this::getReadCompacted).to(spec::setReadCompacted);
			map.from(this::getKeyHashRanges).as(List::of).to(spec::setKeyHashRanges);

			return spec;
		}

	}

	public static class Cache {

		/** Time period to expire unused entries in the cache. */
		private Duration expireAfterAccess = Duration.ofMinutes(1);

		/** Maximum size of cache (entries). */
		private Long maximumSize = 1000L;

		/** Initial size of cache. */
		private Integer initialCapacity = 50;

		public Duration getExpireAfterAccess() {
			return this.expireAfterAccess;
		}

		public void setExpireAfterAccess(Duration expireAfterAccess) {
			this.expireAfterAccess = expireAfterAccess;
		}

		public Long getMaximumSize() {
			return this.maximumSize;
		}

		public void setMaximumSize(Long maximumSize) {
			this.maximumSize = maximumSize;
		}

		public Integer getInitialCapacity() {
			return this.initialCapacity;
		}

		public void setInitialCapacity(Integer initialCapacity) {
			this.initialCapacity = initialCapacity;
		}

	}

}
