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

package org.springframework.pulsar.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.pulsar.core.CachingPulsarProducerFactory.ProducerCacheKey;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.ObjectUtils;

import com.github.benmanes.caffeine.cache.Cache;

/**
 * Tests for {@link CachingPulsarProducerFactory}.
 *
 * @author Chris Bono
 */
class CachingPulsarProducerFactoryTests extends PulsarProducerFactoryTests {

	private List<CachingPulsarProducerFactory<String>> producerFactories;

	@BeforeEach
	void prepareForTests() {
		producerFactories = new ArrayList<>();
	}

	@AfterEach
	void cleanupFromTests() {
		producerFactories.forEach(CachingPulsarProducerFactory::destroy);
	}

	@Test
	void createProducerMultipleCalls() throws PulsarClientException {
		PulsarProducerFactory<String> producerFactory = producerFactory(pulsarClient, Collections.emptyMap());
		ProducerCacheKey<String> cacheKey = new ProducerCacheKey<>(schema, "topic1", null);

		Producer<String> producer1 = producerFactory.createProducer("topic1", schema);
		Producer<String> producer2 = producerFactory.createProducer("topic1", new StringSchema());
		Producer<String> producer3 = producerFactory.createProducer("topic1", new StringSchema());
		assertThat(producer1).isSameAs(producer2).isSameAs(producer3);

		Cache<ProducerCacheKey<String>, Producer<String>> producerCache = getAssertedProducerCache(producerFactory,
				Collections.singletonList(cacheKey));
		Producer<String> cachedProducerProxy = producerCache.asMap().get(cacheKey);
		assertThat(cachedProducerProxy).isSameAs(producer1);
	}

	@Test
	void cachedProducerIsCloseSafeProxy() throws PulsarClientException {
		PulsarProducerFactory<String> producerFactory = producerFactory(pulsarClient, Collections.emptyMap());

		Producer<String> proxyProducer = producerFactory.createProducer("topic1", schema);
		Producer<String> actualProducer = actualProducerFrom(proxyProducer);

		assertThat(actualProducer.isConnected()).isTrue();
		proxyProducer.close();
		assertThat(actualProducer.isConnected()).isTrue();
		actualProducer.close();
		assertThat(actualProducer.isConnected()).isFalse();
	}

	@Test
	void createProducerWithMatrixOfCacheKeys() throws PulsarClientException {
		String topic1 = "topic1";
		String topic2 = "topic2";
		Schema<String> schema1 = new StringSchema();
		Schema<String> schema2 = new StringSchema();
		MessageRouter router1 = mock(MessageRouter.class);
		MessageRouter router2 = mock(MessageRouter.class);

		PulsarProducerFactory<String> producerFactory = producerFactory(pulsarClient, Collections.emptyMap());

		// ask for the same 9 unique combos 3x - should end up w/ only 9 entries in cache
		for (int i = 0; i < 3; i++) {
			producerFactory.createProducer(topic1, schema1);
			producerFactory.createProducer(topic1, schema1, router1);
			producerFactory.createProducer(topic1, schema1, router2);
			producerFactory.createProducer(topic1, schema2);
			producerFactory.createProducer(topic1, schema2, router1);
			producerFactory.createProducer(topic1, schema2, router2);
			producerFactory.createProducer(topic2, schema1);
			producerFactory.createProducer(topic2, schema1, router1);
			producerFactory.createProducer(topic2, schema1, router2);
		}

		List<ProducerCacheKey<String>> expectedCacheKeys = new ArrayList<>();
		expectedCacheKeys.add(new ProducerCacheKey<>(schema1, topic1, null));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema1, topic1, router1));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema1, topic1, router2));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema1, topic2, null));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema1, topic2, router1));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema1, topic2, router2));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema2, topic1, null));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema2, topic1, router1));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema2, topic1, router2));

		getAssertedProducerCache(producerFactory, expectedCacheKeys);
	}

	@Test
	void factoryDestroyCleansUpCacheAndClosesProducers() throws PulsarClientException {
		CachingPulsarProducerFactory<String> producerFactory = producerFactory(pulsarClient, Collections.emptyMap());
		ProducerCacheKey<String> cacheKey1 = new ProducerCacheKey<>(schema, "topic1", null);
		ProducerCacheKey<String> cacheKey2 = new ProducerCacheKey<>(schema, "topic2", null);

		Producer<String> actualProducer1 = actualProducerFrom(producerFactory.createProducer("topic1", schema));
		Producer<String> actualProducer2 = actualProducerFrom(producerFactory.createProducer("topic2", schema));

		Cache<ProducerCacheKey<String>, Producer<String>> producerCache = getAssertedProducerCache(producerFactory,
				Arrays.asList(cacheKey1, cacheKey2));
		producerFactory.destroy();
		Awaitility.await().timeout(Duration.ofSeconds(5L)).untilAsserted(() -> {
			assertThat(producerCache.asMap()).isEmpty();
			assertThat(actualProducer1.isConnected()).isFalse();
			assertThat(actualProducer2.isConnected()).isFalse();
		});
	}

	@Test
	void producerEvictedFromCache() throws PulsarClientException {
		CachingPulsarProducerFactory<String> producerFactory = new CachingPulsarProducerFactory<>(pulsarClient,
				Collections.emptyMap(), Duration.ofSeconds(3L), 10L, 2);
		ProducerCacheKey<String> cacheKey = new ProducerCacheKey<>(schema, "topic1", null);

		Producer<String> actualProducer = actualProducerFrom(producerFactory.createProducer("topic1", schema));

		Cache<ProducerCacheKey<String>, Producer<String>> producerCache = getAssertedProducerCache(producerFactory,
				Collections.singletonList(cacheKey));
		Awaitility.await().pollDelay(Duration.ofSeconds(5L)).timeout(Duration.ofSeconds(10L)).untilAsserted(() -> {
			assertThat(producerCache.asMap()).isEmpty();
			assertThat(actualProducer.isConnected()).isFalse();
		});
	}

	@Test
	void createProducerEncountersException() {
		pulsarClient = spy(pulsarClient);
		when(this.pulsarClient.newProducer(schema)).thenThrow(new RuntimeException("5150"));
		PulsarProducerFactory<String> producerFactory = producerFactory(pulsarClient, Collections.emptyMap());
		assertThatThrownBy(() -> producerFactory.createProducer("topic1", schema)).isInstanceOf(RuntimeException.class)
				.hasMessage("5150");
		getAssertedProducerCache(producerFactory, Collections.emptyList());
	}

	@Override
	protected void assertProducerHasTopicSchemaAndRouter(Producer<String> producer, String topic, Schema<String> schema,
			MessageRouter router) {
		super.assertProducerHasTopicSchemaAndRouter(actualProducerFrom(producer), topic, schema, router);
	}

	@SuppressWarnings("unchecked")
	private Producer<String> actualProducerFrom(Producer<String> proxyProducer) {
		Producer<String> actualProducer = (Producer<String>) AopProxyUtils.getSingletonTarget(proxyProducer);
		assertThat(actualProducer).isNotNull();
		return actualProducer;
	}

	@SuppressWarnings("unchecked")
	private Cache<ProducerCacheKey<String>, Producer<String>> getAssertedProducerCache(
			PulsarProducerFactory<String> producerFactory, List<ProducerCacheKey<String>> expectedCacheKeys) {
		Cache<ProducerCacheKey<String>, Producer<String>> producerCache = (Cache<ProducerCacheKey<String>, Producer<String>>) ReflectionTestUtils
				.getField(producerFactory, "producerCache");
		assertThat(producerCache).isNotNull();
		if (ObjectUtils.isEmpty(expectedCacheKeys)) {
			assertThat(producerCache.asMap()).isEmpty();
		}
		else {
			assertThat(producerCache.asMap()).containsOnlyKeys(expectedCacheKeys);
		}
		return producerCache;
	}

	@Override
	protected CachingPulsarProducerFactory<String> producerFactory(PulsarClient pulsarClient,
			Map<String, Object> producerConfig) {
		CachingPulsarProducerFactory<String> producerFactory = new CachingPulsarProducerFactory<>(pulsarClient,
				producerConfig, Duration.ofMinutes(5L), 10L, 2);
		producerFactories.add(producerFactory);
		return producerFactory;
	}

	@Nested
	class ProducerCacheKeyTests {

		@Test
		void nullSchemaIsNotAllowed() {
			assertThatThrownBy(() -> new ProducerCacheKey<>(null, "topic1", null))
					.isInstanceOf(IllegalArgumentException.class).hasMessage("'schema' must be non-null");
		}

		@Test
		void nullTopicIsNotAllowed() {
			assertThatThrownBy(() -> new ProducerCacheKey<>(schema, null, null))
					.isInstanceOf(IllegalArgumentException.class).hasMessage("'topic' must be non-null");
		}

		@ParameterizedTest(name = "equals({0}) should be {2}")
		@MethodSource("equalsAndHashCodeTestProvider")
		void equalsAndHashCodeTest(Object key1, Object key2, boolean shouldBeEquals) {
			assertThat(key1.equals(key2)).isEqualTo(shouldBeEquals);
			if (shouldBeEquals) {
				assertThat(key1.hashCode()).isEqualTo(key2.hashCode());
			}
		}

		static Stream<Arguments> equalsAndHashCodeTestProvider() {
			MessageRouter router1 = mock(MessageRouter.class);
			ProducerCacheKey<String> key1 = new ProducerCacheKey<>(Schema.STRING, "topic1", router1);
			return Stream.of(arguments(Named.of("differentClass", key1), "someStrangeObject", false),
					arguments(Named.of("null", key1), null, false),
					arguments(Named.of("sameInstance", key1), key1, true),
					arguments(
							Named.of("sameSchemaSameTopicSameNullRouter",
									new ProducerCacheKey<>(Schema.STRING, "topic1", null)),
							new ProducerCacheKey<>(Schema.STRING, "topic1", null), true),
					arguments(
							Named.of("sameSchemaSameTopicSameNonNullRouter",
									new ProducerCacheKey<>(Schema.STRING, "topic1", router1)),
							new ProducerCacheKey<>(Schema.STRING, "topic1", router1), true),
					arguments(
							Named.of("differentSchemaInstanceSameSchemaType",
									new ProducerCacheKey<>(new StringSchema(), "topic1", router1)),
							new ProducerCacheKey<>(new StringSchema(), "topic1", router1), true),
					arguments(Named.of("differentSchemaType", new ProducerCacheKey<>(Schema.STRING, "topic1", router1)),
							new ProducerCacheKey<>(Schema.INT64, "topic1", router1), false),
					arguments(Named.of("differentTopic", new ProducerCacheKey<>(Schema.STRING, "topic1", router1)),
							new ProducerCacheKey<>(Schema.STRING, "topic2", router1), false),
					arguments(
							Named.of("differentNonNullRouter",
									new ProducerCacheKey<>(Schema.STRING, "topic1", router1)),
							new ProducerCacheKey<>(Schema.STRING, "topic1", mock(MessageRouter.class)), false),
					arguments(Named.of("differentNullRouter", new ProducerCacheKey<>(Schema.STRING, "topic1", router1)),
							new ProducerCacheKey<>(Schema.STRING, "topic1", null), false));
		}

	}

}
