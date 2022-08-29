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
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.pulsar.listener.PulsarContainerProperties;

/**
 * Configuration properties for Spring for Apache Pulsar.
 * <p>
 * Users should refer to Pulsar documentation for complete descriptions of these
 * properties.
 *
 * @author Soby Chacko
 * @author Alexander Preuß
 */
@ConfigurationProperties(prefix = "spring.pulsar")
public class PulsarProperties {

	private final Consumer consumer = new Consumer();

	private final Client client = new Client();

	private final Listener listener = new Listener();

	private final Producer producer = new Producer();

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

		private String[] topics;

		private String topicsPattern;

		private String subscriptionName;

		private SubscriptionType subscriptionType = SubscriptionType.Exclusive;

		private int receiverQueueSize = 1000;

		private long acknowledgementsGroupTimeMicros = TimeUnit.MILLISECONDS.toMicros(100);

		private long negativeAckRedeliveryDelayMicros = TimeUnit.MINUTES.toMicros(1);

		private int maxTotalReceiverQueueSizeAcrossPartitions = 50000;

		private String consumerName;

		private long ackTimeoutMillis = 0;

		private long tickDurationMillis = 1000;

		private int priorityLevel = 0;

		private ConsumerCryptoFailureAction cryptoFailureAction = ConsumerCryptoFailureAction.FAIL;

		private SortedMap<String, String> properties = new TreeMap<>();

		private boolean readCompacted = false;

		private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Latest;

		private int patternAutoDiscoveryPeriod = 1;

		private RegexSubscriptionMode regexSubscriptionMode = RegexSubscriptionMode.PersistentOnly;

		private boolean autoUpdatePartitions = true;

		private boolean replicateSubscriptionState = false;

		private boolean autoAckOldestChunkedMessageOnQueueFull = true;

		private int maxPendingChunkedMessage = 10;

		private long expireTimeOfIncompleteChunkedMessageMillis = 60000;

		public String[] getTopics() {
			return this.topics;
		}

		public void setTopics(String[] topics) {
			this.topics = topics;
		}

		public String getTopicsPattern() {
			return this.topicsPattern;
		}

		public void setTopicsPattern(String topicsPattern) {
			this.topicsPattern = topicsPattern;
		}

		public String getSubscriptionName() {
			return this.subscriptionName;
		}

		public void setSubscriptionName(String subscriptionName) {
			this.subscriptionName = subscriptionName;
		}

		public SubscriptionType getSubscriptionType() {
			return this.subscriptionType;
		}

		public void setSubscriptionType(SubscriptionType subscriptionType) {
			this.subscriptionType = subscriptionType;
		}

		public int getReceiverQueueSize() {
			return this.receiverQueueSize;
		}

		public void setReceiverQueueSize(int receiverQueueSize) {
			this.receiverQueueSize = receiverQueueSize;
		}

		public long getAcknowledgementsGroupTimeMicros() {
			return this.acknowledgementsGroupTimeMicros;
		}

		public void setAcknowledgementsGroupTimeMicros(long acknowledgementsGroupTimeMicros) {
			this.acknowledgementsGroupTimeMicros = acknowledgementsGroupTimeMicros;
		}

		public long getNegativeAckRedeliveryDelayMicros() {
			return this.negativeAckRedeliveryDelayMicros;
		}

		public void setNegativeAckRedeliveryDelayMicros(long negativeAckRedeliveryDelayMicros) {
			this.negativeAckRedeliveryDelayMicros = negativeAckRedeliveryDelayMicros;
		}

		public int getMaxTotalReceiverQueueSizeAcrossPartitions() {
			return this.maxTotalReceiverQueueSizeAcrossPartitions;
		}

		public void setMaxTotalReceiverQueueSizeAcrossPartitions(int maxTotalReceiverQueueSizeAcrossPartitions) {
			this.maxTotalReceiverQueueSizeAcrossPartitions = maxTotalReceiverQueueSizeAcrossPartitions;
		}

		public String getConsumerName() {
			return this.consumerName;
		}

		public void setConsumerName(String consumerName) {
			this.consumerName = consumerName;
		}

		public long getAckTimeoutMillis() {
			return this.ackTimeoutMillis;
		}

		public void setAckTimeoutMillis(long ackTimeoutMillis) {
			this.ackTimeoutMillis = ackTimeoutMillis;
		}

		public long getTickDurationMillis() {
			return this.tickDurationMillis;
		}

		public void setTickDurationMillis(long tickDurationMillis) {
			this.tickDurationMillis = tickDurationMillis;
		}

		public int getPriorityLevel() {
			return this.priorityLevel;
		}

		public void setPriorityLevel(int priorityLevel) {
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

		public boolean isReadCompacted() {
			return this.readCompacted;
		}

		public void setReadCompacted(boolean readCompacted) {
			this.readCompacted = readCompacted;
		}

		public SubscriptionInitialPosition getSubscriptionInitialPosition() {
			return this.subscriptionInitialPosition;
		}

		public void setSubscriptionInitialPosition(SubscriptionInitialPosition subscriptionInitialPosition) {
			this.subscriptionInitialPosition = subscriptionInitialPosition;
		}

		public int getPatternAutoDiscoveryPeriod() {
			return this.patternAutoDiscoveryPeriod;
		}

		public void setPatternAutoDiscoveryPeriod(int patternAutoDiscoveryPeriod) {
			this.patternAutoDiscoveryPeriod = patternAutoDiscoveryPeriod;
		}

		public RegexSubscriptionMode getRegexSubscriptionMode() {
			return this.regexSubscriptionMode;
		}

		public void setRegexSubscriptionMode(RegexSubscriptionMode regexSubscriptionMode) {
			this.regexSubscriptionMode = regexSubscriptionMode;
		}

		public boolean isAutoUpdatePartitions() {
			return this.autoUpdatePartitions;
		}

		public void setAutoUpdatePartitions(boolean autoUpdatePartitions) {
			this.autoUpdatePartitions = autoUpdatePartitions;
		}

		public boolean isReplicateSubscriptionState() {
			return this.replicateSubscriptionState;
		}

		public void setReplicateSubscriptionState(boolean replicateSubscriptionState) {
			this.replicateSubscriptionState = replicateSubscriptionState;
		}

		public boolean isAutoAckOldestChunkedMessageOnQueueFull() {
			return this.autoAckOldestChunkedMessageOnQueueFull;
		}

		public void setAutoAckOldestChunkedMessageOnQueueFull(boolean autoAckOldestChunkedMessageOnQueueFull) {
			this.autoAckOldestChunkedMessageOnQueueFull = autoAckOldestChunkedMessageOnQueueFull;
		}

		public int getMaxPendingChunkedMessage() {
			return this.maxPendingChunkedMessage;
		}

		public void setMaxPendingChunkedMessage(int maxPendingChunkedMessage) {
			this.maxPendingChunkedMessage = maxPendingChunkedMessage;
		}

		public long getExpireTimeOfIncompleteChunkedMessageMillis() {
			return this.expireTimeOfIncompleteChunkedMessageMillis;
		}

		public void setExpireTimeOfIncompleteChunkedMessageMillis(long expireTimeOfIncompleteChunkedMessageMillis) {
			this.expireTimeOfIncompleteChunkedMessageMillis = expireTimeOfIncompleteChunkedMessageMillis;
		}

		public Map<String, Object> buildProperties() {
			PulsarProperties.Properties properties = new Properties();

			PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();

			map.from(this::getTopics).as(Set::of).to(properties.in("topicNames"));
			map.from(this::getTopicsPattern).as(Pattern::compile).to(properties.in("topicsPattern"));
			map.from(this::getSubscriptionName).to(properties.in("subscriptionName"));
			map.from(this::getSubscriptionType).to(properties.in("subscriptionType"));
			map.from(this::getReceiverQueueSize).to(properties.in("receiverQueueSize"));
			map.from(this::getAcknowledgementsGroupTimeMicros).to(properties.in("acknowledgementsGroupTimeMicros"));
			map.from(this::getNegativeAckRedeliveryDelayMicros).to(properties.in("negativeAckRedeliveryDelayMicros"));
			map.from(this::getMaxTotalReceiverQueueSizeAcrossPartitions)
					.to(properties.in("maxTotalReceiverQueueSizeAcrossPartitions"));
			map.from(this::getConsumerName).to(properties.in("consumerName"));
			map.from(this::getAckTimeoutMillis).to(properties.in("ackTimeoutMillis"));
			map.from(this::getTickDurationMillis).to(properties.in("tickDurationMillis"));
			map.from(this::getPriorityLevel).to(properties.in("priorityLevel"));
			map.from(this::getCryptoFailureAction).to(properties.in("cryptoFailureAction"));
			map.from(this::getProperties).to(properties.in("properties"));
			map.from(this::isReadCompacted).to(properties.in("readCompacted"));
			map.from(this::getSubscriptionInitialPosition).to(properties.in("subscriptionInitialPosition"));
			map.from(this::getPatternAutoDiscoveryPeriod).to(properties.in("patternAutoDiscoveryPeriod"));
			map.from(this::getRegexSubscriptionMode).to(properties.in("regexSubscriptionMode"));
			map.from(this::isAutoUpdatePartitions).to(properties.in("autoUpdatePartitions"));
			map.from(this::isReplicateSubscriptionState).to(properties.in("replicateSubscriptionState"));
			map.from(this::isAutoAckOldestChunkedMessageOnQueueFull)
					.to(properties.in("autoAckOldestChunkedMessageOnQueueFull"));
			map.from(this::getMaxPendingChunkedMessage).to(properties.in("maxPendingChunkedMessage"));
			map.from(this::getExpireTimeOfIncompleteChunkedMessageMillis)
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
		 * Time before a message has to be acknowledged by the broker in milliseconds.
		 */
		private long sendTimeoutMs = 30000;

		/**
		 * Whether the "send" and "sendAsync" methods should block if the outgoing message
		 * queue is full.
		 */
		private boolean blockIfQueueFull = false;

		/**
		 * Maximum number of pending messages for the producer.
		 */
		private int maxPendingMessages = 1000;

		/**
		 * Maximum number of pending messages across all the partitions.
		 */
		private int maxPendingMessagesAcrossPartitions = 50000;

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
		private long batchingMaxPublishDelayMicros = TimeUnit.MILLISECONDS.toMicros(1);

		/**
		 * Maximum number of messages to be batched.
		 */
		private int batchingMaxMessages = 1000;

		/**
		 * Whether to automatically batch messages.
		 */
		private boolean batchingEnabled = true;

		/**
		 * Whether to split large-size messages into multiple chunks.
		 */
		private boolean chunkingEnabled = false;

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

		public long getSendTimeoutMs() {
			return this.sendTimeoutMs;
		}

		public void setSendTimeoutMs(long sendTimeoutMs) {
			this.sendTimeoutMs = sendTimeoutMs;
		}

		public boolean isBlockIfQueueFull() {
			return this.blockIfQueueFull;
		}

		public void setBlockIfQueueFull(boolean blockIfQueueFull) {
			this.blockIfQueueFull = blockIfQueueFull;
		}

		public int getMaxPendingMessages() {
			return this.maxPendingMessages;
		}

		public void setMaxPendingMessages(int maxPendingMessages) {
			this.maxPendingMessages = maxPendingMessages;
		}

		public int getMaxPendingMessagesAcrossPartitions() {
			return this.maxPendingMessagesAcrossPartitions;
		}

		public void setMaxPendingMessagesAcrossPartitions(int maxPendingMessagesAcrossPartitions) {
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

		public long getBatchingMaxPublishDelayMicros() {
			return this.batchingMaxPublishDelayMicros;
		}

		public void setBatchingMaxPublishDelayMicros(long batchingMaxPublishDelayMicros) {
			this.batchingMaxPublishDelayMicros = batchingMaxPublishDelayMicros;
		}

		public int getBatchingMaxMessages() {
			return this.batchingMaxMessages;
		}

		public void setBatchingMaxMessages(int batchingMaxMessages) {
			this.batchingMaxMessages = batchingMaxMessages;
		}

		public boolean isBatchingEnabled() {
			return this.batchingEnabled;
		}

		public void setBatchingEnabled(boolean batchingEnabled) {
			this.batchingEnabled = batchingEnabled;
		}

		public boolean isChunkingEnabled() {
			return this.chunkingEnabled;
		}

		public void setChunkingEnabled(boolean chunkingEnabled) {
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

		public Map<String, Object> buildProperties() {
			PulsarProperties.Properties properties = new Properties();

			PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();

			map.from(this::getTopicName).to(properties.in("topicName"));
			map.from(this::getProducerName).to(properties.in("producerName"));
			map.from(this::getSendTimeoutMs).to(properties.in("sendTimeoutMs"));
			map.from(this::isBlockIfQueueFull).to(properties.in("blockIfQueueFull"));
			map.from(this::getMaxPendingMessages).to(properties.in("maxPendingMessages"));
			map.from(this::getMaxPendingMessagesAcrossPartitions)
					.to(properties.in("maxPendingMessagesAcrossPartitions"));
			map.from(this::getMessageRoutingMode).to(properties.in("messageRoutingMode"));
			map.from(this::getHashingScheme).to(properties.in("hashingScheme"));
			map.from(this::getCryptoFailureAction).to(properties.in("cryptoFailureAction"));
			map.from(this::getBatchingMaxPublishDelayMicros).to(properties.in("batchingMaxPublishDelayMicros"));
			map.from(this::getBatchingMaxMessages).to(properties.in("batchingMaxMessages"));
			map.from(this::isBatchingEnabled).to(properties.in("batchingEnabled"));
			map.from(this::isChunkingEnabled).to(properties.in("chunkingEnabled"));
			map.from(this::getCompressionType).to(properties.in("compressionType"));
			map.from(this::getInitialSubscriptionName).to(properties.in("initialSubscriptionName"));
			map.from(this::getProducerAccessMode).to(properties.in("accessMode"));

			return properties;
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
		 * Class name of the clients' authentication plugin.
		 */
		private String authPluginClassName;

		/**
		 * Authentication parameter(s) of the client.
		 */
		private String authParams;

		/**
		 * Authentication parameter map of the client.
		 */
		private Map<String, String> authParamsMap;

		/**
		 * Client operation timeout in milliseconds.
		 */
		private long operationTimeoutMs = 30000L;

		/**
		 * Client lookup timeout in milliseconds.
		 */
		private long lookupTimeoutMs = -1;

		/**
		 * Number of threads to be used for handling connections to brokers.
		 */
		private int numIoThreads = 1;

		/**
		 * Number of threads to be used for message listeners.
		 *
		 * The listener thread pool is shared across all the consumers and readers that
		 * are using a "listener" model to get messages. For a given consumer, the
		 * listener will always be invoked from the same thread, to ensure ordering.
		 */
		private int numListenerThreads = 1;

		/**
		 * Maximum number of connections that the client will open to a single broker.
		 */
		private int numConnectionsPerBroker = 1;

		/**
		 * Whether to use TCP no-delay flag on the connection, to disable Nagle algorithm.
		 */
		private boolean useTcpNoDelay = true;

		/**
		 * Whether to use TLS encryption on the connection.
		 */
		private boolean useTls = false;

		/**
		 * Whether the hostname is validated when the proxy creates a TLS connection with
		 * brokers.
		 */
		private boolean tlsHostnameVerificationEnable = false;

		/**
		 * Path to the trusted TLS certificate file.
		 */
		private String tlsTrustCertsFilePath;

		/**
		 * Whether the client accepts untrusted TLS certificates from the broker.
		 */
		private boolean tlsAllowInsecureConnection = false;

		/**
		 * Enable KeyStore instead of PEM type configuration if TLS is enabled.
		 */
		private boolean useKeyStoreTls = false;

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
		private String[] tlsCiphers;

		/**
		 * Comma-separated list of SSL protocols used to generate the SSLContext. Allowed
		 * values in recent JVMs are TLS, TLSv1.3, TLSv1.2 and TLSv1.1.
		 */
		private String[] tlsProtocols;

		/**
		 * Interval between each stat info in seconds.
		 */
		private long statsIntervalSeconds = 60;

		/**
		 * Number of concurrent lookup-requests allowed to send on each broker-connection
		 * to prevent overload on broker.
		 */
		private int maxConcurrentLookupRequest = 5000;

		/**
		 * Number of max lookup-requests allowed on each broker-connection to prevent
		 * overload on broker.
		 */
		private int maxLookupRequest = 50000;

		/**
		 * Maximum number of times a lookup-request to a broker will be redirected.
		 */
		private int maxLookupRedirects = 20;

		/**
		 * Maximum number of broker-rejected requests in a certain timeframe, after which
		 * the current connection is closed and a new connection is created by the client.
		 */
		private int maxNumberOfRejectedRequestPerConnection = 50;

		/**
		 * Keep alive interval for broker-client connection in seconds.
		 */
		private int keepAliveIntervalSeconds = 30;

		/**
		 * Duration to wait for a connection to a broker to be established in
		 * milliseconds.
		 */
		private int connectionTimeoutMs = 10000;

		/**
		 * Initial backoff interval in nanoseconds.
		 */
		private long initialBackoffIntervalNanos = TimeUnit.MILLISECONDS.toNanos(100);

		/**
		 * Maximum backoff interval in nanoseconds.
		 */
		private long maxBackoffIntervalNanos = TimeUnit.SECONDS.toNanos(30);

		/**
		 * Enables spin-waiting on executors and IO threads in order to reduce latency
		 * during context switches.
		 */
		private boolean enableBusyWait = false;

		/**
		 * Limit of direct memory that will be allocated by the client.
		 */
		private long memoryLimitBytes = 64 * 1024 * 1024;

		/**
		 * Enables transactions. To use this, start the transactionCoordinatorClient with
		 * the pulsar client.
		 */
		private boolean enableTransaction = false;

		/**
		 * DNS lookup bind address.
		 */
		private String dnsLookupBindAddress;

		/**
		 * DNS lookup bind port.
		 */
		private int dnsLookupBindPort = 0;

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

		public Map<String, String> getAuthParamsMap() {
			return this.authParamsMap;
		}

		public void setAuthParamsMap(Map<String, String> authParamsMap) {
			this.authParamsMap = authParamsMap;
		}

		public long getOperationTimeoutMs() {
			return this.operationTimeoutMs;
		}

		public void setOperationTimeoutMs(long operationTimeoutMs) {
			this.operationTimeoutMs = operationTimeoutMs;
		}

		public long getLookupTimeoutMs() {
			return this.lookupTimeoutMs;
		}

		public void setLookupTimeoutMs(long lookupTimeoutMs) {
			this.lookupTimeoutMs = lookupTimeoutMs;
		}

		public int getNumIoThreads() {
			return this.numIoThreads;
		}

		public void setNumIoThreads(int numIoThreads) {
			this.numIoThreads = numIoThreads;
		}

		public int getNumListenerThreads() {
			return this.numListenerThreads;
		}

		public void setNumListenerThreads(int numListenerThreads) {
			this.numListenerThreads = numListenerThreads;
		}

		public int getNumConnectionsPerBroker() {
			return this.numConnectionsPerBroker;
		}

		public void setNumConnectionsPerBroker(int numConnectionsPerBroker) {
			this.numConnectionsPerBroker = numConnectionsPerBroker;
		}

		public boolean isUseTcpNoDelay() {
			return this.useTcpNoDelay;
		}

		public void setUseTcpNoDelay(boolean useTcpNoDelay) {
			this.useTcpNoDelay = useTcpNoDelay;
		}

		public boolean isUseTls() {
			return this.useTls;
		}

		public void setUseTls(boolean useTls) {
			this.useTls = useTls;
		}

		public boolean isTlsHostnameVerificationEnable() {
			return this.tlsHostnameVerificationEnable;
		}

		public void setTlsHostnameVerificationEnable(boolean tlsHostnameVerificationEnable) {
			this.tlsHostnameVerificationEnable = tlsHostnameVerificationEnable;
		}

		public String getTlsTrustCertsFilePath() {
			return this.tlsTrustCertsFilePath;
		}

		public void setTlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
			this.tlsTrustCertsFilePath = tlsTrustCertsFilePath;
		}

		public boolean isTlsAllowInsecureConnection() {
			return this.tlsAllowInsecureConnection;
		}

		public void setTlsAllowInsecureConnection(boolean tlsAllowInsecureConnection) {
			this.tlsAllowInsecureConnection = tlsAllowInsecureConnection;
		}

		public boolean isUseKeyStoreTls() {
			return this.useKeyStoreTls;
		}

		public void setUseKeyStoreTls(boolean useKeyStoreTls) {
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

		public String[] getTlsCiphers() {
			return this.tlsCiphers;
		}

		public void setTlsCiphers(String[] tlsCiphers) {
			this.tlsCiphers = tlsCiphers;
		}

		public String[] getTlsProtocols() {
			return this.tlsProtocols;
		}

		public void setTlsProtocols(String[] tlsProtocols) {
			this.tlsProtocols = tlsProtocols;
		}

		public long getStatsIntervalSeconds() {
			return this.statsIntervalSeconds;
		}

		public void setStatsIntervalSeconds(long statsIntervalSeconds) {
			this.statsIntervalSeconds = statsIntervalSeconds;
		}

		public int getMaxConcurrentLookupRequest() {
			return this.maxConcurrentLookupRequest;
		}

		public void setMaxConcurrentLookupRequest(int maxConcurrentLookupRequest) {
			this.maxConcurrentLookupRequest = maxConcurrentLookupRequest;
		}

		public int getMaxLookupRequest() {
			return this.maxLookupRequest;
		}

		public void setMaxLookupRequest(int maxLookupRequest) {
			this.maxLookupRequest = maxLookupRequest;
		}

		public int getMaxLookupRedirects() {
			return this.maxLookupRedirects;
		}

		public void setMaxLookupRedirects(int maxLookupRedirects) {
			this.maxLookupRedirects = maxLookupRedirects;
		}

		public int getMaxNumberOfRejectedRequestPerConnection() {
			return this.maxNumberOfRejectedRequestPerConnection;
		}

		public void setMaxNumberOfRejectedRequestPerConnection(int maxNumberOfRejectedRequestPerConnection) {
			this.maxNumberOfRejectedRequestPerConnection = maxNumberOfRejectedRequestPerConnection;
		}

		public int getKeepAliveIntervalSeconds() {
			return this.keepAliveIntervalSeconds;
		}

		public void setKeepAliveIntervalSeconds(int keepAliveIntervalSeconds) {
			this.keepAliveIntervalSeconds = keepAliveIntervalSeconds;
		}

		public int getConnectionTimeoutMs() {
			return this.connectionTimeoutMs;
		}

		public void setConnectionTimeoutMs(int connectionTimeoutMs) {
			this.connectionTimeoutMs = connectionTimeoutMs;
		}

		public long getInitialBackoffIntervalNanos() {
			return this.initialBackoffIntervalNanos;
		}

		public void setInitialBackoffIntervalNanos(long initialBackoffIntervalNanos) {
			this.initialBackoffIntervalNanos = initialBackoffIntervalNanos;
		}

		public long getMaxBackoffIntervalNanos() {
			return this.maxBackoffIntervalNanos;
		}

		public void setMaxBackoffIntervalNanos(long maxBackoffIntervalNanos) {
			this.maxBackoffIntervalNanos = maxBackoffIntervalNanos;
		}

		public boolean isEnableBusyWait() {
			return this.enableBusyWait;
		}

		public void setEnableBusyWait(boolean enableBusyWait) {
			this.enableBusyWait = enableBusyWait;
		}

		public long getMemoryLimitBytes() {
			return this.memoryLimitBytes;
		}

		public void setMemoryLimitBytes(long memoryLimitBytes) {
			this.memoryLimitBytes = memoryLimitBytes;
		}

		public boolean isEnableTransaction() {
			return this.enableTransaction;
		}

		public void setEnableTransaction(boolean enableTransaction) {
			this.enableTransaction = enableTransaction;
		}

		public String getDnsLookupBindAddress() {
			return this.dnsLookupBindAddress;
		}

		public void setDnsLookupBindAddress(String dnsLookupBindAddress) {
			this.dnsLookupBindAddress = dnsLookupBindAddress;
		}

		public int getDnsLookupBindPort() {
			return this.dnsLookupBindPort;
		}

		public void setDnsLookupBindPort(int dnsLookupBindPort) {
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
			PulsarProperties.Properties properties = new Properties();

			PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();

			map.from(this::getServiceUrl).to(properties.in("serviceUrl"));
			map.from(this::getListenerName).to(properties.in("listenerName"));
			map.from(this::getAuthPluginClassName).to(properties.in("authPluginClassName"));
			map.from(this::getAuthParams).to(properties.in("authParams"));
			map.from(this::getAuthParamsMap).to(properties.in("authParamMap"));
			map.from(this::getOperationTimeoutMs).to(properties.in("operationTimeoutMs"));
			map.from(this::getLookupTimeoutMs).to(properties.in("lookupTimeoutMs"));
			map.from(this::getNumIoThreads).to(properties.in("numIoThreads"));
			map.from(this::getNumListenerThreads).to(properties.in("numListenerThreads"));
			map.from(this::getNumConnectionsPerBroker).to(properties.in("connectionsPerBroker"));
			map.from(this::isUseTcpNoDelay).to(properties.in("useTcpNoDelay"));
			map.from(this::isUseTls).to(properties.in("useTls"));
			map.from(this::isTlsHostnameVerificationEnable).to(properties.in("tlsHostnameVerificationEnable"));
			map.from(this::getTlsTrustCertsFilePath).to(properties.in("tlsTrustCertsFilePath"));
			map.from(this::isTlsAllowInsecureConnection).to(properties.in("tlsAllowInsecureConnection"));
			map.from(this::isUseKeyStoreTls).to(properties.in("useKeyStoreTls"));
			map.from(this::getSslProvider).to(properties.in("sslProvider"));
			map.from(this::getTlsTrustStoreType).to(properties.in("tlsTrustStoreType"));
			map.from(this::getTlsTrustStorePath).to(properties.in("tlsTrustStorePath"));
			map.from(this::getTlsTrustStorePassword).to(properties.in("tlsTrustStorePassword"));
			map.from(this::getTlsCiphers).to(properties.in("tlsCiphers"));
			map.from(this::getTlsProtocols).to(properties.in("tlsProtocols"));
			map.from(this::getStatsIntervalSeconds).to(properties.in("statsIntervalSeconds"));
			map.from(this::getMaxConcurrentLookupRequest).to(properties.in("concurrentLookupRequest"));
			map.from(this::getMaxLookupRequest).to(properties.in("maxLookupRequest"));
			map.from(this::getMaxLookupRedirects).to(properties.in("maxLookupRedirects"));
			map.from(this::getMaxNumberOfRejectedRequestPerConnection)
					.to(properties.in("maxNumberOfRejectedRequestPerConnection"));
			map.from(this::getKeepAliveIntervalSeconds).to(properties.in("keepAliveIntervalSeconds"));
			map.from(this::getConnectionTimeoutMs).to(properties.in("connectionTimeoutMs"));
			map.from(this::getInitialBackoffIntervalNanos).to(properties.in("initialBackoffIntervalNanos"));
			map.from(this::getMaxBackoffIntervalNanos).to(properties.in("maxBackoffIntervalNanos"));
			map.from(this::isEnableBusyWait).to(properties.in("enableBusyWait"));
			map.from(this::getMemoryLimitBytes).to(properties.in("memoryLimitBytes"));
			map.from(this::isEnableTransaction).to(properties.in("enableTransaction"));
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
		private PulsarContainerProperties.AckMode ackMode;

		/**
		 * SchemaType of the consumed messages.
		 */
		private SchemaType schemaType;

		public PulsarContainerProperties.AckMode getAckMode() {
			return this.ackMode;
		}

		public void setAckMode(PulsarContainerProperties.AckMode ackMode) {
			this.ackMode = ackMode;
		}

		public SchemaType getSchemaType() {
			return this.schemaType;
		}

		public void setSchemaType(SchemaType schemaType) {
			this.schemaType = schemaType;
		}

	}

	public static class Admin {

		/**
		 * Pulsar service URL for the admin endpoint.
		 */
		private String serviceUrl;

		/**
		 * Class name of the clients' authentication plugin.
		 */
		private String authPluginClassName;

		/**
		 * Authentication parameter(s) of the client.
		 */
		private String authParams;

		/**
		 * Authentication parameter map of the client.
		 */
		private Map<String, String> authParamMap;

		/**
		 * Path to the trusted TLS certificate file.
		 */
		private String tlsTrustCertsFilePath;

		/**
		 * Whether the client accepts untrusted TLS certificates from the broker.
		 */
		private boolean tlsAllowInsecureConnection = false;

		/**
		 * Whether the hostname is validated when the proxy creates a TLS connection with
		 * brokers.
		 */
		private boolean tlsHostnameVerificationEnable = false;

		/**
		 * Enable KeyStore instead of PEM type configuration if TLS is enabled.
		 */
		private boolean useKeyStoreTls = false;

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
		private String[] tlsCiphers;

		/**
		 * Comma-separated list of SSL protocols used to generate the SSLContext. Allowed
		 * values in recent JVMs are TLS, TLSv1.3, TLSv1.2 and TLSv1.1.
		 */
		private String[] tlsProtocols;

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

		public Map<String, String> getAuthParamMap() {
			return this.authParamMap;
		}

		public void setAuthParamMap(Map<String, String> authParamMap) {
			this.authParamMap = authParamMap;
		}

		public String getTlsTrustCertsFilePath() {
			return this.tlsTrustCertsFilePath;
		}

		public void setTlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
			this.tlsTrustCertsFilePath = tlsTrustCertsFilePath;
		}

		public boolean isTlsAllowInsecureConnection() {
			return this.tlsAllowInsecureConnection;
		}

		public void setTlsAllowInsecureConnection(boolean tlsAllowInsecureConnection) {
			this.tlsAllowInsecureConnection = tlsAllowInsecureConnection;
		}

		public boolean isTlsHostnameVerificationEnable() {
			return this.tlsHostnameVerificationEnable;
		}

		public void setTlsHostnameVerificationEnable(boolean tlsHostnameVerificationEnable) {
			this.tlsHostnameVerificationEnable = tlsHostnameVerificationEnable;
		}

		public boolean isUseKeyStoreTls() {
			return this.useKeyStoreTls;
		}

		public void setUseKeyStoreTls(boolean useKeyStoreTls) {
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

		public String[] getTlsCiphers() {
			return this.tlsCiphers;
		}

		public void setTlsCiphers(String[] tlsCiphers) {
			this.tlsCiphers = tlsCiphers;
		}

		public String[] getTlsProtocols() {
			return this.tlsProtocols;
		}

		public void setTlsProtocols(String[] tlsProtocols) {
			this.tlsProtocols = tlsProtocols;
		}

		public Map<String, Object> buildProperties() {
			PulsarProperties.Properties properties = new Properties();

			PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();

			map.from(this::getServiceUrl).to(properties.in("serviceUrl"));
			map.from(this::getAuthPluginClassName).to(properties.in("authPluginClassName"));
			map.from(this::getAuthParams).to(properties.in("authParams"));
			map.from(this::getAuthParamMap).to(properties.in("authParamMap"));
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
