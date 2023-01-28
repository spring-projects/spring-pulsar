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
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatRuntimeException;

import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.pulsar.autoconfigure.PulsarProperties.TypeMapping;

/**
 * Unit tests for {@link PulsarProperties}.
 *
 * @author Chris Bono
 * @author Christophe Bornet
 */
public class PulsarPropertiesTests {

	private final PulsarProperties properties = new PulsarProperties();

	private void bind(Map<String, String> map) {
		ConfigurationPropertySource source = new MapConfigurationPropertySource(map);
		new Binder(source).bind("spring.pulsar", Bindable.ofInstance(this.properties));
	}

	@Nested
	class ClientPropertiesTests {

		private final String authPluginClassName = "org.apache.pulsar.client.impl.auth.AuthenticationToken";

		private final String authParamsStr = "{\"token\":\"1234\"}";

		private final String authToken = "1234";

		@Test
		void clientProperties() {
			Map<String, String> props = new HashMap<>();
			props.put("spring.pulsar.client.service-url", "my-service-url");
			props.put("spring.pulsar.client.listener-name", "my-listener");
			props.put("spring.pulsar.client.operation-timeout", "1s");
			props.put("spring.pulsar.client.lookup-timeout", "2s");
			props.put("spring.pulsar.client.num-io-threads", "3");
			props.put("spring.pulsar.client.num-listener-threads", "4");
			props.put("spring.pulsar.client.num-connections-per-broker", "5");
			props.put("spring.pulsar.client.use-tcp-no-delay", "false");
			props.put("spring.pulsar.client.use-tls", "true");
			props.put("spring.pulsar.client.tls-hostname-verification-enable", "true");
			props.put("spring.pulsar.client.tls-trust-certs-file-path", "my-trust-certs-file-path");
			props.put("spring.pulsar.client.tls-allow-insecure-connection", "true");
			props.put("spring.pulsar.client.use-key-store-tls", "true");
			props.put("spring.pulsar.client.ssl-provider", "my-ssl-provider");
			props.put("spring.pulsar.client.tls-trust-store-type", "my-trust-store-type");
			props.put("spring.pulsar.client.tls-trust-store-path", "my-trust-store-path");
			props.put("spring.pulsar.client.tls-trust-store-password", "my-trust-store-password");
			props.put("spring.pulsar.client.tls-ciphers[0]", "my-tls-cipher");
			props.put("spring.pulsar.client.tls-protocols[0]", "my-tls-protocol");
			props.put("spring.pulsar.client.stats-interval", "6s");
			props.put("spring.pulsar.client.max-concurrent-lookup-request", "7");
			props.put("spring.pulsar.client.max-lookup-request", "8");
			props.put("spring.pulsar.client.max-lookup-redirects", "9");
			props.put("spring.pulsar.client.max-number-of-rejected-request-per-connection", "10");
			props.put("spring.pulsar.client.keep-alive-interval", "11s");
			props.put("spring.pulsar.client.connection-timeout", "12s");
			props.put("spring.pulsar.client.request-timeout", "13s");
			props.put("spring.pulsar.client.initial-backoff-interval", "14s");
			props.put("spring.pulsar.client.max-backoff-interval", "15s");
			props.put("spring.pulsar.client.enable-busy-wait", "true");
			props.put("spring.pulsar.client.memory-limit", "16B");
			props.put("spring.pulsar.client.proxy-service-url", "my-proxy-service-url");
			props.put("spring.pulsar.client.proxy-protocol", "sni");
			props.put("spring.pulsar.client.enable-transaction", "true");
			props.put("spring.pulsar.client.dns-lookup-bind-address", "my-dns-lookup-bind-address");
			props.put("spring.pulsar.client.dns-lookup-bind-port", "17");
			props.put("spring.pulsar.client.socks5-proxy-address", "my-socks5-proxy-address");
			props.put("spring.pulsar.client.socks5-proxy-username", "my-socks5-proxy-username");
			props.put("spring.pulsar.client.socks5-proxy-password", "my-socks5-proxy-password");

			bind(props);
			Map<String, Object> clientProps = properties.buildClientProperties();

			// Verify that the props can be loaded in a ClientBuilder
			assertThatNoException().isThrownBy(() -> PulsarClient.builder().loadConf(clientProps));

			assertThat(clientProps).containsEntry("serviceUrl", "my-service-url")
					.containsEntry("listenerName", "my-listener").containsEntry("operationTimeoutMs", 1_000L)
					.containsEntry("lookupTimeoutMs", 2_000L).containsEntry("numIoThreads", 3)
					.containsEntry("numListenerThreads", 4).containsEntry("connectionsPerBroker", 5)
					.containsEntry("useTcpNoDelay", false).containsEntry("useTls", true)
					.containsEntry("tlsHostnameVerificationEnable", true)
					.containsEntry("tlsTrustCertsFilePath", "my-trust-certs-file-path")
					.containsEntry("tlsAllowInsecureConnection", true).containsEntry("useKeyStoreTls", true)
					.containsEntry("sslProvider", "my-ssl-provider")
					.containsEntry("tlsTrustStoreType", "my-trust-store-type")
					.containsEntry("tlsTrustStorePath", "my-trust-store-path")
					.containsEntry("tlsTrustStorePassword", "my-trust-store-password")
					.hasEntrySatisfying("tlsCiphers",
							ciphers -> assertThat(ciphers)
									.asInstanceOf(InstanceOfAssertFactories.collection(String.class))
									.containsExactly("my-tls-cipher"))
					.hasEntrySatisfying("tlsProtocols",
							protocols -> assertThat(protocols)
									.asInstanceOf(InstanceOfAssertFactories.collection(String.class))
									.containsExactly("my-tls-protocol"))
					.containsEntry("statsIntervalSeconds", 6L).containsEntry("concurrentLookupRequest", 7)
					.containsEntry("maxLookupRequest", 8).containsEntry("maxLookupRedirects", 9)
					.containsEntry("maxNumberOfRejectedRequestPerConnection", 10)
					.containsEntry("keepAliveIntervalSeconds", 11).containsEntry("connectionTimeoutMs", 12_000)
					.containsEntry("requestTimeoutMs", 13_000)
					.containsEntry("initialBackoffIntervalNanos", 14_000_000_000L)
					.containsEntry("maxBackoffIntervalNanos", 15_000_000_000L).containsEntry("enableBusyWait", true)
					.containsEntry("memoryLimitBytes", 16L).containsEntry("proxyServiceUrl", "my-proxy-service-url")
					.containsEntry("proxyProtocol", ProxyProtocol.SNI).containsEntry("enableTransaction", true)
					.containsEntry("dnsLookupBindAddress", "my-dns-lookup-bind-address")
					.containsEntry("dnsLookupBindPort", 17)
					.containsEntry("socks5ProxyAddress", "my-socks5-proxy-address")
					.containsEntry("socks5ProxyUsername", "my-socks5-proxy-username")
					.containsEntry("socks5ProxyPassword", "my-socks5-proxy-password");
		}

		@Test
		void authenticationUsingAuthParamsString() {
			Map<String, String> props = new HashMap<>();
			props.put("spring.pulsar.client.auth-plugin-class-name",
					"org.apache.pulsar.client.impl.auth.AuthenticationToken");
			props.put("spring.pulsar.client.auth-params", authParamsStr);
			bind(props);
			assertThat(properties.getClient().getAuthParams()).isEqualTo(authParamsStr);
			assertThat(properties.getClient().getAuthPluginClassName()).isEqualTo(authPluginClassName);
			Map<String, Object> clientProps = properties.buildClientProperties();

			assertThat(clientProps).containsEntry("authPluginClassName", authPluginClassName)
					.containsEntry("authParams", authParamsStr);
		}

		@Test
		void authenticationUsingAuthenticationMap() {
			Map<String, String> props = new HashMap<>();
			props.put("spring.pulsar.client.auth-plugin-class-name", authPluginClassName);
			props.put("spring.pulsar.client.authentication.token", authToken);
			bind(props);
			assertThat(properties.getClient().getAuthentication()).containsEntry("token", authToken);
			assertThat(properties.getClient().getAuthPluginClassName()).isEqualTo(authPluginClassName);
			Map<String, Object> clientProps = properties.buildClientProperties();
			assertThat(clientProps).containsEntry("authPluginClassName", authPluginClassName)
					.containsEntry("authParams", authParamsStr);
		}

		@Test
		void authenticationNotAllowedUsingBothAuthParamsStringAndAuthenticationMap() {
			Map<String, String> props = new HashMap<>();
			props.put("spring.pulsar.client.auth-plugin-class-name", authPluginClassName);
			props.put("spring.pulsar.client.auth-params", authParamsStr);
			props.put("spring.pulsar.client.authentication.token", authToken);
			bind(props);
			assertThatIllegalArgumentException().isThrownBy(properties::buildClientProperties).withMessageContaining(
					"Cannot set both spring.pulsar.client.authParams and spring.pulsar.client.authentication.*");
		}

	}

	@Nested
	class AdminPropertiesTests {

		private final String authPluginClassName = "org.apache.pulsar.client.impl.auth.AuthenticationToken";

		private final String authParamsStr = "{\"token\":\"1234\"}";

		private final String authToken = "1234";

		@Test
		void adminProperties() {
			Map<String, String> props = new HashMap<>();
			props.put("spring.pulsar.administration.service-url", "my-service-url");
			props.put("spring.pulsar.administration.connection-timeout", "12s");
			props.put("spring.pulsar.administration.read-timeout", "13s");
			props.put("spring.pulsar.administration.request-timeout", "14s");
			props.put("spring.pulsar.administration.auto-cert-refresh-time", "15s");
			props.put("spring.pulsar.administration.tls-hostname-verification-enable", "true");
			props.put("spring.pulsar.administration.tls-trust-certs-file-path", "my-trust-certs-file-path");
			props.put("spring.pulsar.administration.tls-allow-insecure-connection", "true");
			props.put("spring.pulsar.administration.use-key-store-tls", "true");
			props.put("spring.pulsar.administration.ssl-provider", "my-ssl-provider");
			props.put("spring.pulsar.administration.tls-trust-store-type", "my-trust-store-type");
			props.put("spring.pulsar.administration.tls-trust-store-path", "my-trust-store-path");
			props.put("spring.pulsar.administration.tls-trust-store-password", "my-trust-store-password");
			props.put("spring.pulsar.administration.tls-ciphers[0]", "my-tls-cipher");
			props.put("spring.pulsar.administration.tls-protocols[0]", "my-tls-protocol");

			bind(props);
			Map<String, Object> adminProps = properties.buildAdminProperties();

			// Verify that the props can NOT be loaded directly via a ClientBuilder due to
			// the
			// unknown readTimeout and autoCertRefreshTime properties
			assertThatRuntimeException().isThrownBy(() -> PulsarAdmin.builder().loadConf(adminProps)).havingCause()
					.withMessageContaining("Unrecognized field \"autoCertRefreshSeconds\"");

			assertThat(adminProps).containsEntry("serviceUrl", "my-service-url")
					.containsEntry("connectionTimeoutMs", 12_000).containsEntry("readTimeoutMs", 13_000)
					.containsEntry("requestTimeoutMs", 14_000).containsEntry("autoCertRefreshSeconds", 15)
					.containsEntry("tlsHostnameVerificationEnable", true)
					.containsEntry("tlsTrustCertsFilePath", "my-trust-certs-file-path")
					.containsEntry("tlsAllowInsecureConnection", true).containsEntry("useKeyStoreTls", true)
					.containsEntry("sslProvider", "my-ssl-provider")
					.containsEntry("tlsTrustStoreType", "my-trust-store-type")
					.containsEntry("tlsTrustStorePath", "my-trust-store-path")
					.containsEntry("tlsTrustStorePassword", "my-trust-store-password")
					.hasEntrySatisfying("tlsCiphers",
							ciphers -> assertThat(ciphers)
									.asInstanceOf(InstanceOfAssertFactories.collection(String.class))
									.containsExactly("my-tls-cipher"))
					.hasEntrySatisfying("tlsProtocols",
							protocols -> assertThat(protocols)
									.asInstanceOf(InstanceOfAssertFactories.collection(String.class))
									.containsExactly("my-tls-protocol"));
		}

		@Test
		void authenticationUsingAuthParamsString() {
			Map<String, String> props = new HashMap<>();
			props.put("spring.pulsar.administration.auth-plugin-class-name",
					"org.apache.pulsar.client.impl.auth.AuthenticationToken");
			props.put("spring.pulsar.administration.auth-params", authParamsStr);
			bind(props);
			assertThat(properties.getAdministration().getAuthParams()).isEqualTo(authParamsStr);
			assertThat(properties.getAdministration().getAuthPluginClassName()).isEqualTo(authPluginClassName);
			Map<String, Object> adminProps = properties.buildAdminProperties();
			assertThat(adminProps).containsEntry("authPluginClassName", authPluginClassName).containsEntry("authParams",
					authParamsStr);
		}

		@Test
		void authenticationUsingAuthenticationMap() {
			Map<String, String> props = new HashMap<>();
			props.put("spring.pulsar.administration.auth-plugin-class-name", authPluginClassName);
			props.put("spring.pulsar.administration.authentication.token", authToken);
			bind(props);
			assertThat(properties.getAdministration().getAuthentication()).containsEntry("token", authToken);
			assertThat(properties.getAdministration().getAuthPluginClassName()).isEqualTo(authPluginClassName);
			Map<String, Object> adminProps = properties.buildAdminProperties();
			assertThat(adminProps).containsEntry("authPluginClassName", authPluginClassName).containsEntry("authParams",
					authParamsStr);
		}

		@Test
		void authenticationNotAllowedUsingBothAuthParamsStringAndAuthenticationMap() {
			Map<String, String> props = new HashMap<>();
			props.put("spring.pulsar.administration.auth-plugin-class-name", authPluginClassName);
			props.put("spring.pulsar.administration.auth-params", authParamsStr);
			props.put("spring.pulsar.administration.authentication.token", authToken);
			bind(props);
			assertThatIllegalArgumentException().isThrownBy(properties::buildAdminProperties).withMessageContaining(
					"Cannot set both spring.pulsar.administration.authParams and spring.pulsar.administration.authentication.*");
		}

	}

	@Nested
	class DefaultsPropertiesTests {

		@Test
		void defaultsTypeMappingsEmptyByDefault() {
			assertThat(properties.getDefaults().getTypeMappings()).isEmpty();
		}

		@Test
		void defaultsTypeMappings() {
			Map<String, String> props = new HashMap<>();
			props.put("spring.pulsar.defaults.type-mappings[0].message-type", Foo.class.getName());
			props.put("spring.pulsar.defaults.type-mappings[0].topic-name", "foo-topic");
			props.put("spring.pulsar.defaults.type-mappings[1].message-type", String.class.getName());
			props.put("spring.pulsar.defaults.type-mappings[1].topic-name", "string-topic");
			bind(props);
			assertThat(properties.getDefaults().getTypeMappings()).hasSize(2).containsExactly(
					new TypeMapping(Foo.class, "foo-topic"), new TypeMapping(String.class, "string-topic"));
		}

		record Foo(String value) {
		}

	}

	@Nested
	class ProducerPropertiesTests {

		@Test
		void producerProperties() {
			Map<String, String> props = new HashMap<>();
			props.put("spring.pulsar.producer.topic-name", "my-topic");
			props.put("spring.pulsar.producer.producer-name", "my-producer");
			props.put("spring.pulsar.producer.send-timeout", "2s");
			props.put("spring.pulsar.producer.block-if-queue-full", "true");
			props.put("spring.pulsar.producer.max-pending-messages", "3");
			props.put("spring.pulsar.producer.max-pending-messages-across-partitions", "4");
			props.put("spring.pulsar.producer.message-routing-mode", "custompartition");
			props.put("spring.pulsar.producer.hashing-scheme", "murmur3_32hash");
			props.put("spring.pulsar.producer.crypto-failure-action", "send");
			props.put("spring.pulsar.producer.batching-max-publish-delay", "5s");
			props.put("spring.pulsar.producer.batching-partition-switch-frequency-by-publish-delay", "6");
			props.put("spring.pulsar.producer.batching-max-messages", "7");
			props.put("spring.pulsar.producer.batching-max-bytes", "8");
			props.put("spring.pulsar.producer.batching-enabled", "false");
			props.put("spring.pulsar.producer.chunking-enabled", "true");
			props.put("spring.pulsar.producer.encryption-keys[0]", "my-key");
			props.put("spring.pulsar.producer.compression-type", "lz4");
			props.put("spring.pulsar.producer.initial-sequence-id", "9");
			props.put("spring.pulsar.producer.producer-access-mode", "exclusive");
			props.put("spring.pulsar.producer.lazy-start=partitioned-producers", "true");
			props.put("spring.pulsar.producer.properties[my-prop]", "my-prop-value");

			bind(props);
			Map<String, Object> producerProps = properties.buildProducerProperties();

			// Verify that the props can be loaded in a ProducerBuilder
			assertThatNoException().isThrownBy(() -> ConfigurationDataUtils.loadData(producerProps,
					new ProducerConfigurationData(), ProducerConfigurationData.class));

			assertThat(producerProps).containsEntry("topicName", "my-topic")
					.containsEntry("producerName", "my-producer").containsEntry("sendTimeoutMs", 2_000)
					.containsEntry("blockIfQueueFull", true).containsEntry("maxPendingMessages", 3)
					.containsEntry("maxPendingMessagesAcrossPartitions", 4)
					.containsEntry("messageRoutingMode", MessageRoutingMode.CustomPartition)
					.containsEntry("hashingScheme", HashingScheme.Murmur3_32Hash)
					.containsEntry("cryptoFailureAction", ProducerCryptoFailureAction.SEND)
					.containsEntry("batchingMaxPublishDelayMicros", 5_000_000L)
					.containsEntry("batchingPartitionSwitchFrequencyByPublishDelay", 6)
					.containsEntry("batchingMaxMessages", 7).containsEntry("batchingMaxBytes", 8)
					.containsEntry("batchingEnabled", false).containsEntry("chunkingEnabled", true)
					.hasEntrySatisfying("encryptionKeys",
							keys -> assertThat(keys).asInstanceOf(InstanceOfAssertFactories.collection(String.class))
									.containsExactly("my-key"))
					.containsEntry("compressionType", CompressionType.LZ4).containsEntry("initialSequenceId", 9L)
					.containsEntry("accessMode", ProducerAccessMode.Exclusive)
					.containsEntry("lazyStartPartitionedProducers", true).hasEntrySatisfying("properties",
							properties -> assertThat(properties)
									.asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class))
									.containsEntry("my-prop", "my-prop-value"));
		}

	}

	@Nested
	class ConsumerPropertiesTests {

		@Test
		void consumerProperties() {
			Map<String, String> props = new HashMap<>();
			props.put("spring.pulsar.consumer.topics[0]", "my-topic");
			props.put("spring.pulsar.consumer.topics-pattern", "my-pattern");
			props.put("spring.pulsar.consumer.subscription-name", "my-subscription");
			props.put("spring.pulsar.consumer.subscription-type", "shared");
			props.put("spring.pulsar.consumer.subscription-properties[my-sub-prop]", "my-sub-prop-value");
			props.put("spring.pulsar.consumer.subscription-mode", "nondurable");
			props.put("spring.pulsar.consumer.receiver-queue-size", "1");
			props.put("spring.pulsar.consumer.acknowledgements-group-time", "2s");
			props.put("spring.pulsar.consumer.negative-ack-redelivery-delay", "3s");
			props.put("spring.pulsar.consumer.max-total-receiver-queue-size-across-partitions", "5");
			props.put("spring.pulsar.consumer.consumer-name", "my-consumer");
			props.put("spring.pulsar.consumer.ack-timeout", "6s");
			props.put("spring.pulsar.consumer.tick-duration", "7s");
			props.put("spring.pulsar.consumer.priority-level", "8");
			props.put("spring.pulsar.consumer.crypto-failure-action", "discard");
			props.put("spring.pulsar.consumer.properties[my-prop]", "my-prop-value");
			props.put("spring.pulsar.consumer.read-compacted", "true");
			props.put("spring.pulsar.consumer.subscription-initial-position", "earliest");
			props.put("spring.pulsar.consumer.pattern-auto-discovery-period", "9");
			props.put("spring.pulsar.consumer.regex-subscription-mode", "all-topics");
			props.put("spring.pulsar.consumer.dead-letter-policy.max-redeliver-count", "4");
			props.put("spring.pulsar.consumer.dead-letter-policy.retry-letter-topic", "my-retry-topic");
			props.put("spring.pulsar.consumer.dead-letter-policy.dead-letter-topic", "my-dlt-topic");
			props.put("spring.pulsar.consumer.dead-letter-policy.initial-subscription-name", "my-initial-subscription");
			props.put("spring.pulsar.consumer.retry-enable", "true");
			props.put("spring.pulsar.consumer.auto-update-partitions", "false");
			props.put("spring.pulsar.consumer.auto-update-partitions-interval", "10s");
			props.put("spring.pulsar.consumer.replicate-subscription-state", "true");
			props.put("spring.pulsar.consumer.reset-include-head", "true");
			props.put("spring.pulsar.consumer.batch-index-ack-enabled", "true");
			props.put("spring.pulsar.consumer.ack-receipt-enabled", "true");
			props.put("spring.pulsar.consumer.pool-messages", "true");
			props.put("spring.pulsar.consumer.start-paused", "true");
			props.put("spring.pulsar.consumer.auto-ack-oldest-chunked-message-on-queue-full", "false");
			props.put("spring.pulsar.consumer.max-pending-chunked-message", "11");
			props.put("spring.pulsar.consumer.expire-time-of-incomplete-chunked-message", "12s");

			bind(props);
			Map<String, Object> consumerProps = properties.buildConsumerProperties();

			// Verify that the props can be loaded in a ConsumerBuilder
			assertThatNoException().isThrownBy(() -> ConfigurationDataUtils.loadData(consumerProps,
					new ConsumerConfigurationData<>(), ConsumerConfigurationData.class));

			assertThat(consumerProps)
					.hasEntrySatisfying("topicNames",
							topics -> assertThat(topics)
									.asInstanceOf(InstanceOfAssertFactories.collection(String.class))
									.containsExactly("my-topic"))
					.hasEntrySatisfying("topicsPattern", p -> assertThat(p.toString()).isEqualTo("my-pattern"))
					.containsEntry("subscriptionName", "my-subscription")
					.containsEntry("subscriptionType", SubscriptionType.Shared)
					.hasEntrySatisfying("subscriptionProperties",
							properties -> assertThat(properties)
									.asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class))
									.containsEntry("my-sub-prop", "my-sub-prop-value"))
					.containsEntry("subscriptionMode", SubscriptionMode.NonDurable)
					.containsEntry("receiverQueueSize", 1).containsEntry("acknowledgementsGroupTimeMicros", 2_000_000L)
					.containsEntry("negativeAckRedeliveryDelayMicros", 3_000_000L)
					.containsEntry("maxTotalReceiverQueueSizeAcrossPartitions", 5)
					.containsEntry("consumerName", "my-consumer").containsEntry("ackTimeoutMillis", 6_000L)
					.containsEntry("tickDurationMillis", 7_000L).containsEntry("priorityLevel", 8)
					.containsEntry("cryptoFailureAction", ConsumerCryptoFailureAction.DISCARD)
					.hasEntrySatisfying("properties",
							properties -> assertThat(properties)
									.asInstanceOf(InstanceOfAssertFactories.map(String.class, String.class))
									.containsEntry("my-prop", "my-prop-value"))
					.containsEntry("readCompacted", true)
					.containsEntry("subscriptionInitialPosition", SubscriptionInitialPosition.Earliest)
					.containsEntry("patternAutoDiscoveryPeriod", 9)
					.containsEntry("regexSubscriptionMode", RegexSubscriptionMode.AllTopics)
					.hasEntrySatisfying("deadLetterPolicy", dlp -> {
						DeadLetterPolicy deadLetterPolicy = (DeadLetterPolicy) dlp;
						assertThat(deadLetterPolicy.getMaxRedeliverCount()).isEqualTo(4);
						assertThat(deadLetterPolicy.getRetryLetterTopic()).isEqualTo("my-retry-topic");
						assertThat(deadLetterPolicy.getDeadLetterTopic()).isEqualTo("my-dlt-topic");
						assertThat(deadLetterPolicy.getInitialSubscriptionName()).isEqualTo("my-initial-subscription");
					}).containsEntry("retryEnable", true).containsEntry("autoUpdatePartitions", false)
					.containsEntry("autoUpdatePartitionsIntervalSeconds", 10L)
					.containsEntry("replicateSubscriptionState", true).containsEntry("resetIncludeHead", true)
					.containsEntry("batchIndexAckEnabled", true).containsEntry("ackReceiptEnabled", true)
					.containsEntry("poolMessages", true).containsEntry("startPaused", true)
					.containsEntry("autoAckOldestChunkedMessageOnQueueFull", false)
					.containsEntry("maxPendingChunkedMessage", 11)
					.containsEntry("expireTimeOfIncompleteChunkedMessageMillis", 12_000L);
		}

	}

	@Nested
	class FunctionPropertiesTests {

		@Test
		void functionProperties() {
			Map<String, String> props = new HashMap<>();
			bind(props);

			// check defaults
			assertThat(properties.getFunction().getFailFast()).isTrue();
			assertThat(properties.getFunction().getPropagateFailures()).isTrue();
			assertThat(properties.getFunction().getPropagateStopFailures()).isFalse();

			// set values and verify
			props.put("spring.pulsar.function.fail-fast", "false");
			props.put("spring.pulsar.function.propagate-failures", "false");
			props.put("spring.pulsar.function.propagate-stop-failures", "true");
			bind(props);

			assertThat(properties.getFunction().getFailFast()).isFalse();
			assertThat(properties.getFunction().getPropagateFailures()).isFalse();
			assertThat(properties.getFunction().getPropagateStopFailures()).isTrue();
		}

	}

}
