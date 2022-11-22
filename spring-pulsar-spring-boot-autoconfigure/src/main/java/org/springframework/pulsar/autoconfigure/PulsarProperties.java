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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.listener.AckMode;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.unit.DataSize;

/**
 * Configuration properties for Spring for Apache Pulsar.
 * <p>
 * Users should refer to Pulsar documentation for complete descriptions of these
 * properties.
 *
 * @author Soby Chacko
 * @author Alexander Preuß
 * @author Christophe Bornet
 * @author Chris Bono
 */
@ConfigurationProperties(prefix = "spring.pulsar")
public class PulsarProperties {

	private final Consumer consumer = new Consumer();

	private final Client client = new Client();

	private final Listener listener = new Listener();

	private final Producer producer = new Producer();

	private final Template template = new Template();

	private final Admin admin = new Admin();

	public Consumer getConsumer() {
		return this.consumer;
	}

	public Client getClient() {
		return this.client;
	}

	public Listener getListener() {
		return this.listener;
	}

	public Producer getProducer() {
		return this.producer;
	}

	public Template getTemplate() {
		return this.template;
	}

	public Admin getAdministration() {
		return this.admin;
	}

	public Map<String, Object> buildConsumerProperties() {
		return new HashMap<>(this.consumer.buildProperties());
	}

	public Map<String, Object> buildClientProperties() {
		return new HashMap<>(this.client.buildProperties());
	}

	public Map<String, Object> buildProducerProperties() {
		return new HashMap<>(this.producer.buildProperties());
	}

	public Map<String, Object> buildAdminProperties() {
		return new HashMap<>(this.admin.buildProperties());
	}

	public static class Consumer {

		/**
		 * Comma-separated list of topics the consumer subscribes to.
		 */
		private Set<String> topics;

		/**
		 * Pattern for topics the consumer subscribes to.
		 */
		private Pattern topicsPattern;

		/**
		 * Subscription name for the consumer.
		 */
		private String subscriptionName;

		/**
		 * Subscription type to be used when subscribing to a topic.
		 */
		private SubscriptionType subscriptionType = SubscriptionType.Exclusive;

		/**
		 * Map of properties to add to the subscription.
		 */
		private Map<String, String> subscriptionProperties = new HashMap<>();

		/**
		 * Subscription mode to be used when subscribing to the topic.
		 */
		private SubscriptionMode subscriptionMode = SubscriptionMode.Durable;

		/**
		 * Number of messages that can be accumulated before the consumer calls "receive".
		 */
		private Integer receiverQueueSize = 1000;

		/**
		 * Time to group acknowledgements before sending them to the broker.
		 */
		private Duration acknowledgementsGroupTime = Duration.ofMillis(100);

		/**
		 * Delay before re-delivering messages that have failed to be processed.
		 */
		private Duration negativeAckRedeliveryDelay = Duration.ofMinutes(1);

		/**
		 * Maximum number of messages that a consumer can be pushed at once from a broker
		 * across all partitions.
		 */
		private Integer maxTotalReceiverQueueSizeAcrossPartitions = 50000;

		/**
		 * Consumer name to identify a particular consumer from the topic stats.
		 */
		private String consumerName;

		/**
		 * Timeout for unacked messages to be redelivered.
		 */
		private Duration ackTimeout = Duration.ZERO;

		/**
		 * Precision for the ack timeout messages tracker.
		 */
		private Duration tickDuration = Duration.ofSeconds(1);

		/**
		 * Priority level for shared subscription consumers.
		 */
		private Integer priorityLevel = 0;

		/**
		 * Action the consumer will take in case of decryption failure.
		 */
		private ConsumerCryptoFailureAction cryptoFailureAction = ConsumerCryptoFailureAction.FAIL;

		/**
		 * Map of properties to add to the consumer.
		 */
		private SortedMap<String, String> properties = new TreeMap<>();

		/**
		 * Whether to read messages from the compacted topic rather than the full message
		 * backlog.
		 */
		private Boolean readCompacted = false;

		/**
		 * Position where to initialize a newly created subscription.
		 */
		private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Latest;

		/**
		 * Auto-discovery period for topics when topic pattern is used in minutes.
		 */
		private Integer patternAutoDiscoveryPeriod = 1;

		/**
		 * Determines which topics the consumer should be subscribed to when using pattern
		 * subscriptions.
		 */
		private RegexSubscriptionMode regexSubscriptionMode = RegexSubscriptionMode.PersistentOnly;

		/**
		 * Dead letter policy to use.
		 */
		@Nullable
		@NestedConfigurationProperty
		private DeadLetterPolicy deadLetterPolicy;

		/**
		 * Whether to auto retry messages.
		 */
		private Boolean retryEnable = false;

		/**
		 * Whether the consumer auto-subscribes for partition increase. This is only for
		 * partitioned consumers.
		 */
		private Boolean autoUpdatePartitions = true;

		/**
		 * Interval of partitions discovery updates.
		 */
		private Duration autoUpdatePartitionsInterval = Duration.ofMinutes(1);

		/**
		 * Whether to replicate subscription state.
		 */
		private Boolean replicateSubscriptionState = false;

		/**
		 * Whether to include the given position of any reset operation like
		 * {@link org.apache.pulsar.client.api.Consumer#seek(long) or
		 * {@link Consumer#seek(MessageId)}}.
		 */
		private Boolean resetIncludeHead = false;

		/**
		 * Whether the batch index acknowledgment is enabled.
		 */
		private Boolean batchIndexAckEnabled = false;

		/**
		 * Whether an acknowledgement receipt is enabled.
		 */
		private Boolean ackReceiptEnabled = false;

		/**
		 * Whether pooling of messages and the underlying data buffers is enabled.
		 */
		private Boolean poolMessages = false;

		/**
		 * Whether to start the consumer in a paused state.
		 */
		private Boolean startPaused = false;

		/**
		 * Whether to automatically drop outstanding un-acked messages if the queue is
		 * full.
		 */
		private Boolean autoAckOldestChunkedMessageOnQueueFull = true;

		/**
		 * Maximum number of chunked messages to be kept in memory.
		 */
		private Integer maxPendingChunkedMessage = 10;

		/**
		 * Time to expire incomplete chunks if the consumer won't be able to receive all
		 * chunks before.
		 */
		private Duration expireTimeOfIncompleteChunkedMessage = Duration.ofMinutes(1);

		public Set<String> getTopics() {
			return this.topics;
		}

		public void setTopics(Set<String> topics) {
			this.topics = topics;
		}

		public Pattern getTopicsPattern() {
			return this.topicsPattern;
		}

		public void setTopicsPattern(Pattern topicsPattern) {
			this.topicsPattern = topicsPattern;
		}

		public String getSubscriptionName() {
			return this.subscriptionName;
		}

		public void setSubscriptionName(String subscriptionName) {
			this.subscriptionName = subscriptionName;
		}

		public Map<String, String> getSubscriptionProperties() {
			return this.subscriptionProperties;
		}

		public void setSubscriptionProperties(Map<String, String> subscriptionProperties) {
			this.subscriptionProperties = subscriptionProperties;
		}

		public SubscriptionMode getSubscriptionMode() {
			return this.subscriptionMode;
		}

		public void setSubscriptionMode(SubscriptionMode subscriptionMode) {
			this.subscriptionMode = subscriptionMode;
		}

		public SubscriptionType getSubscriptionType() {
			return this.subscriptionType;
		}

		public void setSubscriptionType(SubscriptionType subscriptionType) {
			this.subscriptionType = subscriptionType;
		}

		public Integer getReceiverQueueSize() {
			return this.receiverQueueSize;
		}

		public void setReceiverQueueSize(Integer receiverQueueSize) {
			this.receiverQueueSize = receiverQueueSize;
		}

		public Duration getAcknowledgementsGroupTime() {
			return this.acknowledgementsGroupTime;
		}

		public void setAcknowledgementsGroupTime(Duration acknowledgementsGroupTime) {
			this.acknowledgementsGroupTime = acknowledgementsGroupTime;
		}

		public Duration getNegativeAckRedeliveryDelay() {
			return this.negativeAckRedeliveryDelay;
		}

		public void setNegativeAckRedeliveryDelay(Duration negativeAckRedeliveryDelay) {
			this.negativeAckRedeliveryDelay = negativeAckRedeliveryDelay;
		}

		public Integer getMaxTotalReceiverQueueSizeAcrossPartitions() {
			return this.maxTotalReceiverQueueSizeAcrossPartitions;
		}

		public void setMaxTotalReceiverQueueSizeAcrossPartitions(Integer maxTotalReceiverQueueSizeAcrossPartitions) {
			this.maxTotalReceiverQueueSizeAcrossPartitions = maxTotalReceiverQueueSizeAcrossPartitions;
		}

		public String getConsumerName() {
			return this.consumerName;
		}

		public void setConsumerName(String consumerName) {
			this.consumerName = consumerName;
		}

		public Duration getAckTimeout() {
			return this.ackTimeout;
		}

		public void setAckTimeout(Duration ackTimeout) {
			this.ackTimeout = ackTimeout;
		}

		public Duration getTickDuration() {
			return this.tickDuration;
		}

		public void setTickDuration(Duration tickDuration) {
			this.tickDuration = tickDuration;
		}

		public Integer getPriorityLevel() {
			return this.priorityLevel;
		}

		public void setPriorityLevel(Integer priorityLevel) {
			this.priorityLevel = priorityLevel;
		}

		public ConsumerCryptoFailureAction getCryptoFailureAction() {
			return this.cryptoFailureAction;
		}

		public void setCryptoFailureAction(ConsumerCryptoFailureAction cryptoFailureAction) {
			this.cryptoFailureAction = cryptoFailureAction;
		}

		public SortedMap<String, String> getProperties() {
			return this.properties;
		}

		public void setProperties(SortedMap<String, String> properties) {
			this.properties = properties;
		}

		public Boolean getReadCompacted() {
			return this.readCompacted;
		}

		public void setReadCompacted(Boolean readCompacted) {
			this.readCompacted = readCompacted;
		}

		public SubscriptionInitialPosition getSubscriptionInitialPosition() {
			return this.subscriptionInitialPosition;
		}

		public void setSubscriptionInitialPosition(SubscriptionInitialPosition subscriptionInitialPosition) {
			this.subscriptionInitialPosition = subscriptionInitialPosition;
		}

		public Integer getPatternAutoDiscoveryPeriod() {
			return this.patternAutoDiscoveryPeriod;
		}

		public void setPatternAutoDiscoveryPeriod(Integer patternAutoDiscoveryPeriod) {
			this.patternAutoDiscoveryPeriod = patternAutoDiscoveryPeriod;
		}

		public RegexSubscriptionMode getRegexSubscriptionMode() {
			return this.regexSubscriptionMode;
		}

		public void setRegexSubscriptionMode(RegexSubscriptionMode regexSubscriptionMode) {
			this.regexSubscriptionMode = regexSubscriptionMode;
		}

		@Nullable
		public DeadLetterPolicy getDeadLetterPolicy() {
			return this.deadLetterPolicy;
		}

		public void setDeadLetterPolicy(@Nullable DeadLetterPolicy deadLetterPolicy) {
			this.deadLetterPolicy = deadLetterPolicy;
		}

		public Boolean getRetryEnable() {
			return this.retryEnable;
		}

		public void setRetryEnable(Boolean retryEnable) {
			this.retryEnable = retryEnable;
		}

		public Boolean getAutoUpdatePartitions() {
			return this.autoUpdatePartitions;
		}

		public void setAutoUpdatePartitions(Boolean autoUpdatePartitions) {
			this.autoUpdatePartitions = autoUpdatePartitions;
		}

		public Duration getAutoUpdatePartitionsInterval() {
			return this.autoUpdatePartitionsInterval;
		}

		public void setAutoUpdatePartitionsInterval(Duration autoUpdatePartitionsInterval) {
			this.autoUpdatePartitionsInterval = autoUpdatePartitionsInterval;
		}

		public Boolean getReplicateSubscriptionState() {
			return this.replicateSubscriptionState;
		}

		public void setReplicateSubscriptionState(Boolean replicateSubscriptionState) {
			this.replicateSubscriptionState = replicateSubscriptionState;
		}

		public Boolean getResetIncludeHead() {
			return this.resetIncludeHead;
		}

		public void setResetIncludeHead(Boolean resetIncludeHead) {
			this.resetIncludeHead = resetIncludeHead;
		}

		public Boolean getBatchIndexAckEnabled() {
			return this.batchIndexAckEnabled;
		}

		public void setBatchIndexAckEnabled(Boolean batchIndexAckEnabled) {
			this.batchIndexAckEnabled = batchIndexAckEnabled;
		}

		public Boolean getAckReceiptEnabled() {
			return this.ackReceiptEnabled;
		}

		public void setAckReceiptEnabled(Boolean ackReceiptEnabled) {
			this.ackReceiptEnabled = ackReceiptEnabled;
		}

		public Boolean getPoolMessages() {
			return this.poolMessages;
		}

		public void setPoolMessages(Boolean poolMessages) {
			this.poolMessages = poolMessages;
		}

		public Boolean getStartPaused() {
			return this.startPaused;
		}

		public void setStartPaused(Boolean startPaused) {
			this.startPaused = startPaused;
		}

		public Boolean getAutoAckOldestChunkedMessageOnQueueFull() {
			return this.autoAckOldestChunkedMessageOnQueueFull;
		}

		public void setAutoAckOldestChunkedMessageOnQueueFull(Boolean autoAckOldestChunkedMessageOnQueueFull) {
			this.autoAckOldestChunkedMessageOnQueueFull = autoAckOldestChunkedMessageOnQueueFull;
		}

		public Integer getMaxPendingChunkedMessage() {
			return this.maxPendingChunkedMessage;
		}

		public void setMaxPendingChunkedMessage(Integer maxPendingChunkedMessage) {
			this.maxPendingChunkedMessage = maxPendingChunkedMessage;
		}

		public Duration getExpireTimeOfIncompleteChunkedMessage() {
			return this.expireTimeOfIncompleteChunkedMessage;
		}

		public void setExpireTimeOfIncompleteChunkedMessage(Duration expireTimeOfIncompleteChunkedMessage) {
			this.expireTimeOfIncompleteChunkedMessage = expireTimeOfIncompleteChunkedMessage;
		}

		public Map<String, Object> buildProperties() {
			PulsarProperties.Properties properties = new Properties();

			PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();

			map.from(this::getTopics).to(properties.in("topicNames"));
			map.from(this::getTopicsPattern).to(properties.in("topicsPattern"));
			map.from(this::getSubscriptionName).to(properties.in("subscriptionName"));
			map.from(this::getSubscriptionType).to(properties.in("subscriptionType"));
			map.from(this::getSubscriptionProperties).to(properties.in("subscriptionProperties"));
			map.from(this::getSubscriptionMode).to(properties.in("subscriptionMode"));
			map.from(this::getReceiverQueueSize).to(properties.in("receiverQueueSize"));
			map.from(this::getAcknowledgementsGroupTime).as(it -> it.toNanos() / 1000)
					.to(properties.in("acknowledgementsGroupTimeMicros"));
			map.from(this::getNegativeAckRedeliveryDelay).as(it -> it.toNanos() / 1000)
					.to(properties.in("negativeAckRedeliveryDelayMicros"));
			map.from(this::getMaxTotalReceiverQueueSizeAcrossPartitions)
					.to(properties.in("maxTotalReceiverQueueSizeAcrossPartitions"));
			map.from(this::getConsumerName).to(properties.in("consumerName"));
			map.from(this::getAckTimeout).as(Duration::toMillis).to(properties.in("ackTimeoutMillis"));
			map.from(this::getTickDuration).as(Duration::toMillis).to(properties.in("tickDurationMillis"));
			map.from(this::getPriorityLevel).to(properties.in("priorityLevel"));
			map.from(this::getCryptoFailureAction).to(properties.in("cryptoFailureAction"));
			map.from(this::getProperties).to(properties.in("properties"));
			map.from(this::getReadCompacted).to(properties.in("readCompacted"));
			map.from(this::getSubscriptionInitialPosition).to(properties.in("subscriptionInitialPosition"));
			map.from(this::getPatternAutoDiscoveryPeriod).to(properties.in("patternAutoDiscoveryPeriod"));
			map.from(this::getRegexSubscriptionMode).to(properties.in("regexSubscriptionMode"));
			map.from(this::getDeadLetterPolicy).to(properties.in("deadLetterPolicy"));
			map.from(this::getRetryEnable).to(properties.in("retryEnable"));
			map.from(this::getAutoUpdatePartitions).to(properties.in("autoUpdatePartitions"));
			map.from(this::getAutoUpdatePartitionsInterval).as(Duration::toSeconds)
					.to(properties.in("autoUpdatePartitionsIntervalSeconds"));
			map.from(this::getReplicateSubscriptionState).to(properties.in("replicateSubscriptionState"));
			map.from(this::getResetIncludeHead).to(properties.in("resetIncludeHead"));
			map.from(this::getBatchIndexAckEnabled).to(properties.in("batchIndexAckEnabled"));
			map.from(this::getAckReceiptEnabled).to(properties.in("ackReceiptEnabled"));
			map.from(this::getPoolMessages).to(properties.in("poolMessages"));
			map.from(this::getStartPaused).to(properties.in("startPaused"));
			map.from(this::getAutoAckOldestChunkedMessageOnQueueFull)
					.to(properties.in("autoAckOldestChunkedMessageOnQueueFull"));
			map.from(this::getMaxPendingChunkedMessage).to(properties.in("maxPendingChunkedMessage"));
			map.from(this::getExpireTimeOfIncompleteChunkedMessage).as(Duration::toMillis)
					.to(properties.in("expireTimeOfIncompleteChunkedMessageMillis"));
			return properties;
		}

	}

	public static class Producer {

		/**
		 * Topic the producer will publish to.
		 */
		private String topicName;

		/**
		 * Name for the producer. If not assigned, a unique name is generated.
		 */
		private String producerName;

		/**
		 * Time before a message has to be acknowledged by the broker.
		 */
		private Duration sendTimeout = Duration.ofSeconds(30);

		/**
		 * Whether the "send" and "sendAsync" methods should block if the outgoing message
		 * queue is full.
		 */
		private Boolean blockIfQueueFull = false;

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
		 * Time period within which the messages sent will be batched.
		 */
		private Duration batchingMaxPublishDelay = Duration.ofMillis(1);

		/**
		 * Partition switch frequency while batching of messages is enabled and using
		 * round-robin routing mode for non-keyed message.
		 */
		private Integer batchingPartitionSwitchFrequencyByPublishDelay = 10;

		/**
		 * Maximum number of messages to be batched.
		 */
		private Integer batchingMaxMessages = 1000;

		/**
		 * Maximum number of bytes permitted in a batch.
		 */
		private DataSize batchingMaxBytes = DataSize.ofKilobytes(128);

		/**
		 * Whether to automatically batch messages.
		 */
		private Boolean batchingEnabled = true;

		/**
		 * Whether to split large-size messages into multiple chunks.
		 */
		private Boolean chunkingEnabled = false;

		/**
		 * Names of the public encryption keys to use when encrypting data.
		 */
		private Set<String> encryptionKeys = new HashSet<>();

		/**
		 * Message compression type.
		 */
		private CompressionType compressionType;

		/**
		 * Baseline for the sequence ids for messages published by the producer.
		 */
		@Nullable
		private Long initialSequenceId;

		/**
		 * Whether partitioned producer automatically discover new partitions at runtime.
		 */
		private Boolean autoUpdatePartitions = true;

		/**
		 * Interval of partitions discovery updates.
		 */
		private Duration autoUpdatePartitionsInterval = Duration.ofMinutes(1);

		/**
		 * Whether the multiple schema mode is enabled.
		 */
		private Boolean multiSchema = true;

		/**
		 * Type of access to the topic the producer requires.
		 */
		private ProducerAccessMode producerAccessMode = ProducerAccessMode.Shared;

		/**
		 * Whether producers in Shared mode register and connect immediately to the owner
		 * broker of each partition or start lazily on demand.
		 */
		private Boolean lazyStartPartitionedProducers = false;

		/**
		 * Map of properties to add to the producer.
		 */
		private Map<String, String> properties = new HashMap<>();

		private Cache cache = new Cache();

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

		public Boolean getBlockIfQueueFull() {
			return this.blockIfQueueFull;
		}

		public void setBlockIfQueueFull(Boolean blockIfQueueFull) {
			this.blockIfQueueFull = blockIfQueueFull;
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

		public Integer getBatchingPartitionSwitchFrequencyByPublishDelay() {
			return this.batchingPartitionSwitchFrequencyByPublishDelay;
		}

		public void setBatchingPartitionSwitchFrequencyByPublishDelay(
				Integer batchingPartitionSwitchFrequencyByPublishDelay) {
			this.batchingPartitionSwitchFrequencyByPublishDelay = batchingPartitionSwitchFrequencyByPublishDelay;
		}

		public Integer getBatchingMaxMessages() {
			return this.batchingMaxMessages;
		}

		public void setBatchingMaxMessages(Integer batchingMaxMessages) {
			this.batchingMaxMessages = batchingMaxMessages;
		}

		public DataSize getBatchingMaxBytes() {
			return this.batchingMaxBytes;
		}

		public void setBatchingMaxBytes(DataSize batchingMaxBytes) {
			this.batchingMaxBytes = batchingMaxBytes;
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

		public Set<String> getEncryptionKeys() {
			return this.encryptionKeys;
		}

		public void setEncryptionKeys(Set<String> encryptionKeys) {
			this.encryptionKeys = encryptionKeys;
		}

		public CompressionType getCompressionType() {
			return this.compressionType;
		}

		public void setCompressionType(CompressionType compressionType) {
			this.compressionType = compressionType;
		}

		@Nullable
		public Long getInitialSequenceId() {
			return this.initialSequenceId;
		}

		public void setInitialSequenceId(@Nullable Long initialSequenceId) {
			this.initialSequenceId = initialSequenceId;
		}

		public Boolean getAutoUpdatePartitions() {
			return this.autoUpdatePartitions;
		}

		public void setAutoUpdatePartitions(Boolean autoUpdatePartitions) {
			this.autoUpdatePartitions = autoUpdatePartitions;
		}

		public Duration getAutoUpdatePartitionsInterval() {
			return this.autoUpdatePartitionsInterval;
		}

		public void setAutoUpdatePartitionsInterval(Duration autoUpdatePartitionsInterval) {
			this.autoUpdatePartitionsInterval = autoUpdatePartitionsInterval;
		}

		public Boolean getMultiSchema() {
			return this.multiSchema;
		}

		public void setMultiSchema(Boolean multiSchema) {
			this.multiSchema = multiSchema;
		}

		public ProducerAccessMode getProducerAccessMode() {
			return this.producerAccessMode;
		}

		public void setProducerAccessMode(ProducerAccessMode producerAccessMode) {
			this.producerAccessMode = producerAccessMode;
		}

		public Boolean getLazyStartPartitionedProducers() {
			return this.lazyStartPartitionedProducers;
		}

		public void setLazyStartPartitionedProducers(Boolean lazyStartPartitionedProducers) {
			this.lazyStartPartitionedProducers = lazyStartPartitionedProducers;
		}

		public Map<String, String> getProperties() {
			return this.properties;
		}

		public void setProperties(Map<String, String> properties) {
			this.properties = properties;
		}

		public Cache getCache() {
			return this.cache;
		}

		public Map<String, Object> buildProperties() {
			PulsarProperties.Properties properties = new Properties();

			PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();

			map.from(this::getTopicName).to(properties.in("topicName"));
			map.from(this::getProducerName).to(properties.in("producerName"));
			map.from(this::getSendTimeout).asInt(Duration::toMillis).to(properties.in("sendTimeoutMs"));
			map.from(this::getBlockIfQueueFull).to(properties.in("blockIfQueueFull"));
			map.from(this::getMaxPendingMessages).to(properties.in("maxPendingMessages"));
			map.from(this::getMaxPendingMessagesAcrossPartitions)
					.to(properties.in("maxPendingMessagesAcrossPartitions"));
			map.from(this::getMessageRoutingMode).to(properties.in("messageRoutingMode"));
			map.from(this::getHashingScheme).to(properties.in("hashingScheme"));
			map.from(this::getCryptoFailureAction).to(properties.in("cryptoFailureAction"));
			map.from(this::getBatchingMaxPublishDelay).as(it -> it.toNanos() / 1000)
					.to(properties.in("batchingMaxPublishDelayMicros"));
			map.from(this::getBatchingPartitionSwitchFrequencyByPublishDelay)
					.to(properties.in("batchingPartitionSwitchFrequencyByPublishDelay"));
			map.from(this::getBatchingMaxMessages).to(properties.in("batchingMaxMessages"));
			map.from(this::getBatchingMaxBytes).asInt(DataSize::toBytes).to(properties.in("batchingMaxBytes"));
			map.from(this::getBatchingEnabled).to(properties.in("batchingEnabled"));
			map.from(this::getChunkingEnabled).to(properties.in("chunkingEnabled"));
			map.from(this::getEncryptionKeys).to(properties.in("encryptionKeys"));
			map.from(this::getCompressionType).to(properties.in("compressionType"));
			map.from(this::getInitialSequenceId).to(properties.in("initialSequenceId"));
			map.from(this::getAutoUpdatePartitions).to(properties.in("autoUpdatePartitions"));
			map.from(this::getAutoUpdatePartitionsInterval).as(Duration::toSeconds)
					.to(properties.in("autoUpdatePartitionsIntervalSeconds"));
			map.from(this::getMultiSchema).to(properties.in("multiSchema"));
			map.from(this::getProducerAccessMode).to(properties.in("accessMode"));
			map.from(this::getLazyStartPartitionedProducers).to(properties.in("lazyStartPartitionedProducers"));
			map.from(this::getProperties).to(properties.in("properties"));

			return properties;
		}

	}

	public static class Template {

		/**
		 * Whether to record observations for send operations when the Observations API is
		 * available.
		 */
		private Boolean observationsEnabled = true;

		public Boolean isObservationsEnabled() {
			return this.observationsEnabled;
		}

		public void setObservationsEnabled(Boolean observationsEnabled) {
			this.observationsEnabled = observationsEnabled;
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

	public static class Client {

		/**
		 * Pulsar cluster URL to connect to a broker.
		 */
		private String serviceUrl;

		/**
		 * Listener name for lookup. Clients can use listenerName to choose one of the
		 * listeners as the service URL to create a connection to the broker. To use this,
		 * "advertisedListeners" must be enabled on the broker.
		 */
		private String listenerName;

		/**
		 * Fully qualified class name of the authentication plugin.
		 */
		private String authPluginClassName;

		/**
		 * Authentication parameter(s) as a JSON encoded string.
		 */
		private String authParams;

		/**
		 * Authentication parameter(s) as a map of parameter names to parameter values.
		 */
		private Map<String, String> authentication;

		/**
		 * Client operation timeout.
		 */
		private Duration operationTimeout = Duration.ofSeconds(30);

		/**
		 * Client lookup timeout.
		 */
		private Duration lookupTimeout = Duration.ofMillis(-1);

		/**
		 * Number of threads to be used for handling connections to brokers.
		 */
		private Integer numIoThreads = 1;

		/**
		 * Number of threads to be used for message listeners. The listener thread pool is
		 * shared across all the consumers and readers that are using a "listener" model
		 * to get messages. For a given consumer, the listener will always be invoked from
		 * the same thread, to ensure ordering.
		 */
		private Integer numListenerThreads = 1;

		/**
		 * Maximum number of connections that the client will open to a single broker.
		 */
		private Integer numConnectionsPerBroker = 1;

		/**
		 * Whether to use TCP no-delay flag on the connection, to disable Nagle algorithm.
		 */
		private Boolean useTcpNoDelay = true;

		/**
		 * Whether to use TLS encryption on the connection.
		 */
		private Boolean useTls = false;

		/**
		 * Whether the hostname is validated when the proxy creates a TLS connection with
		 * brokers.
		 */
		private Boolean tlsHostnameVerificationEnable = false;

		/**
		 * Path to the trusted TLS certificate file.
		 */
		private String tlsTrustCertsFilePath;

		/**
		 * Whether the client accepts untrusted TLS certificates from the broker.
		 */
		private Boolean tlsAllowInsecureConnection = false;

		/**
		 * Enable KeyStore instead of PEM type configuration if TLS is enabled.
		 */
		private Boolean useKeyStoreTls = false;

		/**
		 * Name of the security provider used for SSL connections.
		 */
		private String sslProvider;

		/**
		 * File format of the trust store file.
		 */
		private String tlsTrustStoreType;

		/**
		 * Location of the trust store file.
		 */
		private String tlsTrustStorePath;

		/**
		 * Store password for the key store file.
		 */
		private String tlsTrustStorePassword;

		/**
		 * Comma-separated list of cipher suites. This is a named combination of
		 * authentication, encryption, MAC and key exchange algorithm used to negotiate
		 * the security settings for a network connection using TLS or SSL network
		 * protocol. By default, all the available cipher suites are supported.
		 */
		private Set<String> tlsCiphers;

		/**
		 * Comma-separated list of SSL protocols used to generate the SSLContext. Allowed
		 * values in recent JVMs are TLS, TLSv1.3, TLSv1.2 and TLSv1.1.
		 */
		private Set<String> tlsProtocols;

		/**
		 * Interval between each stat info.
		 */
		private Duration statsInterval = Duration.ofSeconds(60);

		/**
		 * Number of concurrent lookup-requests allowed to send on each broker-connection
		 * to prevent overload on broker.
		 */
		private Integer maxConcurrentLookupRequest = 5000;

		/**
		 * Number of max lookup-requests allowed on each broker-connection to prevent
		 * overload on broker.
		 */
		private Integer maxLookupRequest = 50000;

		/**
		 * Maximum number of times a lookup-request to a broker will be redirected.
		 */
		private Integer maxLookupRedirects = 20;

		/**
		 * Maximum number of broker-rejected requests in a certain timeframe, after which
		 * the current connection is closed and a new connection is created by the client.
		 */
		private Integer maxNumberOfRejectedRequestPerConnection = 50;

		/**
		 * Keep alive interval for broker-client connection.
		 */
		private Duration keepAliveInterval = Duration.ofSeconds(30);

		/**
		 * Duration to wait for a connection to a broker to be established.
		 */
		private Duration connectionTimeout = Duration.ofSeconds(10);

		/**
		 * Maximum duration for completing a request.
		 */
		private Duration requestTimeout = Duration.ofMinutes(1);

		/**
		 * Initial backoff interval.
		 */
		private Duration initialBackoffInterval = Duration.ofMillis(100);

		/**
		 * Maximum backoff interval.
		 */
		private Duration maxBackoffInterval = Duration.ofSeconds(30);

		/**
		 * Enables spin-waiting on executors and IO threads in order to reduce latency
		 * during context switches.
		 */
		private Boolean enableBusyWait = false;

		/**
		 * Limit of direct memory that will be allocated by the client.
		 */
		private DataSize memoryLimit = DataSize.ofMegabytes(64);

		/**
		 * URL of proxy service. proxyServiceUrl and proxyProtocol must be mutually
		 * inclusive.
		 */
		private String proxyServiceUrl;

		/**
		 * Protocol of proxy service. proxyServiceUrl and proxyProtocol must be mutually
		 * inclusive.
		 */
		private ProxyProtocol proxyProtocol;

		/**
		 * Enables transactions. To use this, start the transactionCoordinatorClient with
		 * the pulsar client.
		 */
		private Boolean enableTransaction = false;

		/**
		 * DNS lookup bind address.
		 */
		private String dnsLookupBindAddress;

		/**
		 * DNS lookup bind port.
		 */
		private Integer dnsLookupBindPort = 0;

		/**
		 * SOCKS5 proxy address.
		 */
		private String socks5ProxyAddress;

		/**
		 * SOCKS5 proxy username.
		 */
		private String socks5ProxyUsername;

		/**
		 * SOCKS5 proxy password.
		 */
		private String socks5ProxyPassword;

		public String getServiceUrl() {
			return this.serviceUrl;
		}

		public void setServiceUrl(String serviceUrl) {
			this.serviceUrl = serviceUrl;
		}

		public String getListenerName() {
			return this.listenerName;
		}

		public void setListenerName(String listenerName) {
			this.listenerName = listenerName;
		}

		public String getAuthPluginClassName() {
			return this.authPluginClassName;
		}

		public void setAuthPluginClassName(String authPluginClassName) {
			this.authPluginClassName = authPluginClassName;
		}

		public String getAuthParams() {
			return this.authParams;
		}

		public void setAuthParams(String authParams) {
			this.authParams = authParams;
		}

		public Map<String, String> getAuthentication() {
			return this.authentication;
		}

		public void setAuthentication(Map<String, String> authentication) {
			this.authentication = authentication;
		}

		public Duration getOperationTimeout() {
			return this.operationTimeout;
		}

		public void setOperationTimeout(Duration operationTimeout) {
			this.operationTimeout = operationTimeout;
		}

		public Duration getLookupTimeout() {
			return this.lookupTimeout;
		}

		public void setLookupTimeout(Duration lookupTimeout) {
			this.lookupTimeout = lookupTimeout;
		}

		public Integer getNumIoThreads() {
			return this.numIoThreads;
		}

		public void setNumIoThreads(Integer numIoThreads) {
			this.numIoThreads = numIoThreads;
		}

		public Integer getNumListenerThreads() {
			return this.numListenerThreads;
		}

		public void setNumListenerThreads(Integer numListenerThreads) {
			this.numListenerThreads = numListenerThreads;
		}

		public Integer getNumConnectionsPerBroker() {
			return this.numConnectionsPerBroker;
		}

		public void setNumConnectionsPerBroker(Integer numConnectionsPerBroker) {
			this.numConnectionsPerBroker = numConnectionsPerBroker;
		}

		public Boolean getUseTcpNoDelay() {
			return this.useTcpNoDelay;
		}

		public void setUseTcpNoDelay(Boolean useTcpNoDelay) {
			this.useTcpNoDelay = useTcpNoDelay;
		}

		public Boolean getUseTls() {
			return this.useTls;
		}

		public void setUseTls(Boolean useTls) {
			this.useTls = useTls;
		}

		public Boolean getTlsHostnameVerificationEnable() {
			return this.tlsHostnameVerificationEnable;
		}

		public void setTlsHostnameVerificationEnable(Boolean tlsHostnameVerificationEnable) {
			this.tlsHostnameVerificationEnable = tlsHostnameVerificationEnable;
		}

		public String getTlsTrustCertsFilePath() {
			return this.tlsTrustCertsFilePath;
		}

		public void setTlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
			this.tlsTrustCertsFilePath = tlsTrustCertsFilePath;
		}

		public Boolean getTlsAllowInsecureConnection() {
			return this.tlsAllowInsecureConnection;
		}

		public void setTlsAllowInsecureConnection(Boolean tlsAllowInsecureConnection) {
			this.tlsAllowInsecureConnection = tlsAllowInsecureConnection;
		}

		public Boolean getUseKeyStoreTls() {
			return this.useKeyStoreTls;
		}

		public void setUseKeyStoreTls(Boolean useKeyStoreTls) {
			this.useKeyStoreTls = useKeyStoreTls;
		}

		public String getSslProvider() {
			return this.sslProvider;
		}

		public void setSslProvider(String sslProvider) {
			this.sslProvider = sslProvider;
		}

		public String getTlsTrustStoreType() {
			return this.tlsTrustStoreType;
		}

		public void setTlsTrustStoreType(String tlsTrustStoreType) {
			this.tlsTrustStoreType = tlsTrustStoreType;
		}

		public String getTlsTrustStorePath() {
			return this.tlsTrustStorePath;
		}

		public void setTlsTrustStorePath(String tlsTrustStorePath) {
			this.tlsTrustStorePath = tlsTrustStorePath;
		}

		public String getTlsTrustStorePassword() {
			return this.tlsTrustStorePassword;
		}

		public void setTlsTrustStorePassword(String tlsTrustStorePassword) {
			this.tlsTrustStorePassword = tlsTrustStorePassword;
		}

		public Set<String> getTlsCiphers() {
			return this.tlsCiphers;
		}

		public void setTlsCiphers(Set<String> tlsCiphers) {
			this.tlsCiphers = tlsCiphers;
		}

		public Set<String> getTlsProtocols() {
			return this.tlsProtocols;
		}

		public void setTlsProtocols(Set<String> tlsProtocols) {
			this.tlsProtocols = tlsProtocols;
		}

		public Duration getStatsInterval() {
			return this.statsInterval;
		}

		public void setStatsInterval(Duration statsInterval) {
			this.statsInterval = statsInterval;
		}

		public Integer getMaxConcurrentLookupRequest() {
			return this.maxConcurrentLookupRequest;
		}

		public void setMaxConcurrentLookupRequest(Integer maxConcurrentLookupRequest) {
			this.maxConcurrentLookupRequest = maxConcurrentLookupRequest;
		}

		public Integer getMaxLookupRequest() {
			return this.maxLookupRequest;
		}

		public void setMaxLookupRequest(Integer maxLookupRequest) {
			this.maxLookupRequest = maxLookupRequest;
		}

		public Integer getMaxLookupRedirects() {
			return this.maxLookupRedirects;
		}

		public void setMaxLookupRedirects(Integer maxLookupRedirects) {
			this.maxLookupRedirects = maxLookupRedirects;
		}

		public Integer getMaxNumberOfRejectedRequestPerConnection() {
			return this.maxNumberOfRejectedRequestPerConnection;
		}

		public void setMaxNumberOfRejectedRequestPerConnection(Integer maxNumberOfRejectedRequestPerConnection) {
			this.maxNumberOfRejectedRequestPerConnection = maxNumberOfRejectedRequestPerConnection;
		}

		public Duration getKeepAliveInterval() {
			return this.keepAliveInterval;
		}

		public void setKeepAliveInterval(Duration keepAliveInterval) {
			this.keepAliveInterval = keepAliveInterval;
		}

		public Duration getConnectionTimeout() {
			return this.connectionTimeout;
		}

		public void setConnectionTimeout(Duration connectionTimeout) {
			this.connectionTimeout = connectionTimeout;
		}

		public Duration getRequestTimeout() {
			return this.requestTimeout;
		}

		public void setRequestTimeout(Duration requestTimeout) {
			this.requestTimeout = requestTimeout;
		}

		public Duration getInitialBackoffInterval() {
			return this.initialBackoffInterval;
		}

		public void setInitialBackoffInterval(Duration initialBackoffInterval) {
			this.initialBackoffInterval = initialBackoffInterval;
		}

		public Duration getMaxBackoffInterval() {
			return this.maxBackoffInterval;
		}

		public void setMaxBackoffInterval(Duration maxBackoffInterval) {
			this.maxBackoffInterval = maxBackoffInterval;
		}

		public Boolean getEnableBusyWait() {
			return this.enableBusyWait;
		}

		public void setEnableBusyWait(Boolean enableBusyWait) {
			this.enableBusyWait = enableBusyWait;
		}

		public DataSize getMemoryLimit() {
			return this.memoryLimit;
		}

		public void setMemoryLimit(DataSize memoryLimit) {
			this.memoryLimit = memoryLimit;
		}

		public String getProxyServiceUrl() {
			return this.proxyServiceUrl;
		}

		public void setProxyServiceUrl(String proxyServiceUrl) {
			this.proxyServiceUrl = proxyServiceUrl;
		}

		public ProxyProtocol getProxyProtocol() {
			return this.proxyProtocol;
		}

		public void setProxyProtocol(ProxyProtocol proxyProtocol) {
			this.proxyProtocol = proxyProtocol;
		}

		public Boolean getEnableTransaction() {
			return this.enableTransaction;
		}

		public void setEnableTransaction(Boolean enableTransaction) {
			this.enableTransaction = enableTransaction;
		}

		public String getDnsLookupBindAddress() {
			return this.dnsLookupBindAddress;
		}

		public void setDnsLookupBindAddress(String dnsLookupBindAddress) {
			this.dnsLookupBindAddress = dnsLookupBindAddress;
		}

		public Integer getDnsLookupBindPort() {
			return this.dnsLookupBindPort;
		}

		public void setDnsLookupBindPort(Integer dnsLookupBindPort) {
			this.dnsLookupBindPort = dnsLookupBindPort;
		}

		public String getSocks5ProxyAddress() {
			return this.socks5ProxyAddress;
		}

		public void setSocks5ProxyAddress(String socks5ProxyAddress) {
			this.socks5ProxyAddress = socks5ProxyAddress;
		}

		public String getSocks5ProxyUsername() {
			return this.socks5ProxyUsername;
		}

		public void setSocks5ProxyUsername(String socks5ProxyUsername) {
			this.socks5ProxyUsername = socks5ProxyUsername;
		}

		public String getSocks5ProxyPassword() {
			return this.socks5ProxyPassword;
		}

		public void setSocks5ProxyPassword(String socks5ProxyPassword) {
			this.socks5ProxyPassword = socks5ProxyPassword;
		}

		public Map<String, Object> buildProperties() {
			if (StringUtils.hasText(this.getAuthParams()) && !CollectionUtils.isEmpty(this.getAuthentication())) {
				throw new IllegalArgumentException(
						"Cannot set both spring.pulsar.client.authParams and spring.pulsar.client.authentication.*");
			}

			PulsarProperties.Properties properties = new Properties();

			PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();

			map.from(this::getServiceUrl).to(properties.in("serviceUrl"));
			map.from(this::getListenerName).to(properties.in("listenerName"));
			map.from(this::getAuthPluginClassName).to(properties.in("authPluginClassName"));
			map.from(this::getAuthParams).to(properties.in("authParams"));
			map.from(this::getAuthentication).as(AuthParameterUtils::maybeConvertToEncodedParamString)
					.to(properties.in("authParams"));
			map.from(this::getOperationTimeout).as(Duration::toMillis).to(properties.in("operationTimeoutMs"));
			map.from(this::getLookupTimeout).as(Duration::toMillis).to(properties.in("lookupTimeoutMs"));
			map.from(this::getNumIoThreads).to(properties.in("numIoThreads"));
			map.from(this::getNumListenerThreads).to(properties.in("numListenerThreads"));
			map.from(this::getNumConnectionsPerBroker).to(properties.in("connectionsPerBroker"));
			map.from(this::getUseTcpNoDelay).to(properties.in("useTcpNoDelay"));
			map.from(this::getUseTls).to(properties.in("useTls"));
			map.from(this::getTlsHostnameVerificationEnable).to(properties.in("tlsHostnameVerificationEnable"));
			map.from(this::getTlsTrustCertsFilePath).to(properties.in("tlsTrustCertsFilePath"));
			map.from(this::getTlsAllowInsecureConnection).to(properties.in("tlsAllowInsecureConnection"));
			map.from(this::getUseKeyStoreTls).to(properties.in("useKeyStoreTls"));
			map.from(this::getSslProvider).to(properties.in("sslProvider"));
			map.from(this::getTlsTrustStoreType).to(properties.in("tlsTrustStoreType"));
			map.from(this::getTlsTrustStorePath).to(properties.in("tlsTrustStorePath"));
			map.from(this::getTlsTrustStorePassword).to(properties.in("tlsTrustStorePassword"));
			map.from(this::getTlsCiphers).to(properties.in("tlsCiphers"));
			map.from(this::getTlsProtocols).to(properties.in("tlsProtocols"));
			map.from(this::getStatsInterval).as(Duration::toSeconds).to(properties.in("statsIntervalSeconds"));
			map.from(this::getMaxConcurrentLookupRequest).to(properties.in("concurrentLookupRequest"));
			map.from(this::getMaxLookupRequest).to(properties.in("maxLookupRequest"));
			map.from(this::getMaxLookupRedirects).to(properties.in("maxLookupRedirects"));
			map.from(this::getMaxNumberOfRejectedRequestPerConnection)
					.to(properties.in("maxNumberOfRejectedRequestPerConnection"));
			map.from(this::getKeepAliveInterval).asInt(Duration::toSeconds)
					.to(properties.in("keepAliveIntervalSeconds"));
			map.from(this::getConnectionTimeout).asInt(Duration::toMillis).to(properties.in("connectionTimeoutMs"));
			map.from(this::getRequestTimeout).asInt(Duration::toMillis).to(properties.in("requestTimeoutMs"));
			map.from(this::getInitialBackoffInterval).as(Duration::toNanos)
					.to(properties.in("initialBackoffIntervalNanos"));
			map.from(this::getMaxBackoffInterval).as(Duration::toNanos).to(properties.in("maxBackoffIntervalNanos"));
			map.from(this::getEnableBusyWait).to(properties.in("enableBusyWait"));
			map.from(this::getMemoryLimit).as(DataSize::toBytes).to(properties.in("memoryLimitBytes"));
			map.from(this::getProxyServiceUrl).to(properties.in("proxyServiceUrl"));
			map.from(this::getProxyProtocol).to(properties.in("proxyProtocol"));
			map.from(this::getEnableTransaction).to(properties.in("enableTransaction"));
			map.from(this::getDnsLookupBindAddress).to(properties.in("dnsLookupBindAddress"));
			map.from(this::getDnsLookupBindPort).to(properties.in("dnsLookupBindPort"));
			map.from(this::getSocks5ProxyAddress).to(properties.in("socks5ProxyAddress"));
			map.from(this::getSocks5ProxyUsername).to(properties.in("socks5ProxyUsername"));
			map.from(this::getSocks5ProxyPassword).to(properties.in("socks5ProxyPassword"));

			return properties;
		}

	}

	public static class Listener {

		/**
		 * AckMode for acknowledgements. Allowed values are RECORD, BATCH, MANUAL.
		 */
		private AckMode ackMode;

		/**
		 * SchemaType of the consumed messages.
		 */
		private SchemaType schemaType;

		/**
		 * Max number of messages in a single batch request.
		 */
		private Integer maxNumMessages = -1;

		/**
		 * Max size in a single batch request.
		 */
		private DataSize maxNumBytes = DataSize.ofMegabytes(10);

		/**
		 * Duration to wait for enough message to fill a batch request before timing out.
		 */
		private Duration batchTimeout = Duration.ofMillis(100);

		/**
		 * Whether to record observations for receive operations when the Observations API
		 * is available.
		 */
		private Boolean observationsEnabled = true;

		public AckMode getAckMode() {
			return this.ackMode;
		}

		public void setAckMode(AckMode ackMode) {
			this.ackMode = ackMode;
		}

		public SchemaType getSchemaType() {
			return this.schemaType;
		}

		public void setSchemaType(SchemaType schemaType) {
			this.schemaType = schemaType;
		}

		public Integer getMaxNumMessages() {
			return this.maxNumMessages;
		}

		public void setMaxNumMessages(Integer maxNumMessages) {
			this.maxNumMessages = maxNumMessages;
		}

		public DataSize getMaxNumBytes() {
			return this.maxNumBytes;
		}

		public void setMaxNumBytes(DataSize maxNumBytes) {
			this.maxNumBytes = maxNumBytes;
		}

		public Duration getBatchTimeout() {
			return this.batchTimeout;
		}

		public void setBatchTimeout(Duration batchTimeout) {
			this.batchTimeout = batchTimeout;
		}

		public Boolean isObservationsEnabled() {
			return this.observationsEnabled;
		}

		public void setObservationsEnabled(Boolean observationsEnabled) {
			this.observationsEnabled = observationsEnabled;
		}

	}

	public static class Admin {

		/**
		 * Pulsar service URL for the admin endpoint.
		 */
		private String serviceUrl;

		/**
		 * Fully qualified class name of the authentication plugin.
		 */
		private String authPluginClassName;

		/**
		 * Authentication parameter(s) as a JSON encoded string.
		 */
		private String authParams;

		/**
		 * Authentication parameter(s) as a map of parameter names to parameter values.
		 */
		private Map<String, String> authentication;

		/**
		 * Path to the trusted TLS certificate file.
		 */
		private String tlsTrustCertsFilePath;

		/**
		 * Whether the client accepts untrusted TLS certificates from the broker.
		 */
		private Boolean tlsAllowInsecureConnection = false;

		/**
		 * Whether the hostname is validated when the proxy creates a TLS connection with
		 * brokers.
		 */
		private Boolean tlsHostnameVerificationEnable = false;

		/**
		 * Enable KeyStore instead of PEM type configuration if TLS is enabled.
		 */
		private Boolean useKeyStoreTls = false;

		/**
		 * Name of the security provider used for SSL connections.
		 */
		private String sslProvider;

		/**
		 * File format of the trust store file.
		 */
		private String tlsTrustStoreType;

		/**
		 * Location of the trust store file.
		 */
		private String tlsTrustStorePath;

		/**
		 * Store password for the key store file.
		 */
		private String tlsTrustStorePassword;

		/**
		 * List of cipher suites. This is a named combination of authentication,
		 * encryption, MAC and key exchange algorithm used to negotiate the security
		 * settings for a network connection using TLS or SSL network protocol. By
		 * default, all the available cipher suites are supported.
		 */
		private Set<String> tlsCiphers;

		/**
		 * List of SSL protocols used to generate the SSLContext. Allowed values in recent
		 * JVMs are TLS, TLSv1.3, TLSv1.2 and TLSv1.1.
		 */
		private Set<String> tlsProtocols;

		public String getServiceUrl() {
			return this.serviceUrl;
		}

		public void setServiceUrl(String serviceUrl) {
			this.serviceUrl = serviceUrl;
		}

		public String getAuthPluginClassName() {
			return this.authPluginClassName;
		}

		public void setAuthPluginClassName(String authPluginClassName) {
			this.authPluginClassName = authPluginClassName;
		}

		public String getAuthParams() {
			return this.authParams;
		}

		public void setAuthParams(String authParams) {
			this.authParams = authParams;
		}

		public Map<String, String> getAuthentication() {
			return this.authentication;
		}

		public void setAuthentication(Map<String, String> authentication) {
			this.authentication = authentication;
		}

		public String getTlsTrustCertsFilePath() {
			return this.tlsTrustCertsFilePath;
		}

		public void setTlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
			this.tlsTrustCertsFilePath = tlsTrustCertsFilePath;
		}

		public Boolean isTlsAllowInsecureConnection() {
			return this.tlsAllowInsecureConnection;
		}

		public void setTlsAllowInsecureConnection(Boolean tlsAllowInsecureConnection) {
			this.tlsAllowInsecureConnection = tlsAllowInsecureConnection;
		}

		public Boolean isTlsHostnameVerificationEnable() {
			return this.tlsHostnameVerificationEnable;
		}

		public void setTlsHostnameVerificationEnable(Boolean tlsHostnameVerificationEnable) {
			this.tlsHostnameVerificationEnable = tlsHostnameVerificationEnable;
		}

		public Boolean isUseKeyStoreTls() {
			return this.useKeyStoreTls;
		}

		public void setUseKeyStoreTls(Boolean useKeyStoreTls) {
			this.useKeyStoreTls = useKeyStoreTls;
		}

		public String getSslProvider() {
			return this.sslProvider;
		}

		public void setSslProvider(String sslProvider) {
			this.sslProvider = sslProvider;
		}

		public String getTlsTrustStoreType() {
			return this.tlsTrustStoreType;
		}

		public void setTlsTrustStoreType(String tlsTrustStoreType) {
			this.tlsTrustStoreType = tlsTrustStoreType;
		}

		public String getTlsTrustStorePath() {
			return this.tlsTrustStorePath;
		}

		public void setTlsTrustStorePath(String tlsTrustStorePath) {
			this.tlsTrustStorePath = tlsTrustStorePath;
		}

		public String getTlsTrustStorePassword() {
			return this.tlsTrustStorePassword;
		}

		public void setTlsTrustStorePassword(String tlsTrustStorePassword) {
			this.tlsTrustStorePassword = tlsTrustStorePassword;
		}

		public Set<String> getTlsCiphers() {
			return this.tlsCiphers;
		}

		public void setTlsCiphers(Set<String> tlsCiphers) {
			this.tlsCiphers = tlsCiphers;
		}

		public Set<String> getTlsProtocols() {
			return this.tlsProtocols;
		}

		public void setTlsProtocols(Set<String> tlsProtocols) {
			this.tlsProtocols = tlsProtocols;
		}

		public Map<String, Object> buildProperties() {
			if (StringUtils.hasText(this.getAuthParams()) && !CollectionUtils.isEmpty(this.getAuthentication())) {
				throw new IllegalArgumentException(
						"Cannot set both spring.pulsar.administration.authParams and spring.pulsar.administration.authentication.*");
			}
			PulsarProperties.Properties properties = new Properties();

			PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();

			map.from(this::getServiceUrl).to(properties.in("serviceUrl"));
			map.from(this::getAuthPluginClassName).to(properties.in("authPluginClassName"));
			map.from(this::getAuthParams).to(properties.in("authParams"));
			map.from(this::getAuthentication).as(AuthParameterUtils::maybeConvertToEncodedParamString)
					.to(properties.in("authParams"));
			map.from(this::getTlsTrustCertsFilePath).to(properties.in("tlsTrustCertsFilePath"));
			map.from(this::isTlsAllowInsecureConnection).to(properties.in("tlsAllowInsecureConnection"));
			map.from(this::isTlsHostnameVerificationEnable).to(properties.in("tlsHostnameVerificationEnable"));
			map.from(this::isUseKeyStoreTls).to(properties.in("useKeyStoreTls"));
			map.from(this::getSslProvider).to(properties.in("sslProvider"));
			map.from(this::getTlsTrustStoreType).to(properties.in("tlsTrustStoreType"));
			map.from(this::getTlsTrustStorePath).to(properties.in("tlsTrustStorePath"));
			map.from(this::getTlsTrustStorePassword).to(properties.in("tlsTrustStorePassword"));
			map.from(this::getTlsCiphers).to(properties.in("tlsCiphers"));
			map.from(this::getTlsProtocols).to(properties.in("tlsProtocols"));

			properties.putIfAbsent("serviceUrl", "http://localhost:8080");

			return properties;
		}

	}

	@SuppressWarnings("serial")
	private static class Properties extends HashMap<String, Object> {

		<V> java.util.function.Consumer<V> in(String key) {
			return (value) -> put(key, value);
		}

	}

}
