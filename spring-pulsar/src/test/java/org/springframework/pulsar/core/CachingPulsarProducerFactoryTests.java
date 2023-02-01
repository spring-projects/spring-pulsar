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

package org.springframework.pulsar.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

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

import org.springframework.pulsar.core.CachingPulsarProducerFactory.ProducerCacheKey;
import org.springframework.pulsar.core.CachingPulsarProducerFactory.ProducerWithCloseCallback;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.ObjectUtils;

import com.github.benmanes.caffeine.cache.Cache;

/**
 * Tests for {@link CachingPulsarProducerFactory}.
 *
 * @author Chris Bono
 * @author Alexander Preuß
 * @author Christophe Bornet
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
		var producerFactory = newProducerFactory();
		var cacheKey = new ProducerCacheKey<>(schema, "topic1", null, null);
		var producer1 = producerFactory.createProducer(schema, "topic1");
		var producer2 = producerFactory.createProducer(new StringSchema(), "topic1");
		var producer3 = producerFactory.createProducer(new StringSchema(), "topic1");
		assertThat(producer1).isSameAs(producer2).isSameAs(producer3);

		Cache<ProducerCacheKey<String>, Producer<String>> producerCache = getAssertedProducerCache(producerFactory,
				Collections.singletonList(cacheKey));
		Producer<String> cachedProducerWrapper = producerCache.asMap().get(cacheKey);
		assertThat(cachedProducerWrapper).isSameAs(producer1);
	}

	@Test
	void cachedProducerIsCloseSafeWrapper() throws PulsarClientException {
		var producerFactory = newProducerFactory();
		var wrappedProducer = producerFactory.createProducer(schema, "topic1");
		var actualProducer = actualProducer(wrappedProducer);

		assertThat(actualProducer.isConnected()).isTrue();
		wrappedProducer.close();
		assertThat(actualProducer.isConnected()).isTrue();
		actualProducer.close();
		assertThat(actualProducer.isConnected()).isFalse();
	}

	@SuppressWarnings("resource")
	@Test
	void createProducerWithMatrixOfCacheKeys() throws PulsarClientException {
		String topic1 = "topic1";
		String topic2 = "topic2";
		var schema1 = new StringSchema();
		var schema2 = new StringSchema();
		List<ProducerBuilderCustomizer<String>> customizers1 = List.of(p -> p.property("key", "value"));
		List<ProducerBuilderCustomizer<String>> customizers2 = List.of(p -> p.property("key", "value"));
		var encryptionKeys1 = Set.of("key1");
		var encryptionKeys2 = Set.of("key2");
		var producerFactory = newProducerFactory();

		// ask for the same 21 unique combos 3x - should end up w/ only 21 entries in
		// cache
		for (int i = 0; i < 3; i++) {
			producerFactory.createProducer(schema1, topic1);
			producerFactory.createProducer(schema1, topic1, encryptionKeys1, null);
			producerFactory.createProducer(schema1, topic1, encryptionKeys2, null);
			producerFactory.createProducer(schema1, topic1, encryptionKeys1, customizers1);
			producerFactory.createProducer(schema1, topic1, encryptionKeys1, customizers2);
			producerFactory.createProducer(schema1, topic1, encryptionKeys2, customizers1);
			producerFactory.createProducer(schema1, topic1, encryptionKeys2, customizers2);
			producerFactory.createProducer(schema2, topic1);
			producerFactory.createProducer(schema2, topic1, encryptionKeys1, null);
			producerFactory.createProducer(schema2, topic1, encryptionKeys2, null);
			producerFactory.createProducer(schema2, topic1, encryptionKeys1, customizers1);
			producerFactory.createProducer(schema2, topic1, encryptionKeys1, customizers2);
			producerFactory.createProducer(schema2, topic1, encryptionKeys2, customizers1);
			producerFactory.createProducer(schema2, topic1, encryptionKeys2, customizers2);
			producerFactory.createProducer(schema1, topic2);
			producerFactory.createProducer(schema1, topic2, encryptionKeys1, null);
			producerFactory.createProducer(schema1, topic2, encryptionKeys2, null);
			producerFactory.createProducer(schema1, topic2, encryptionKeys1, customizers1);
			producerFactory.createProducer(schema1, topic2, encryptionKeys1, customizers2);
			producerFactory.createProducer(schema1, topic2, encryptionKeys2, customizers1);
			producerFactory.createProducer(schema1, topic2, encryptionKeys2, customizers2);
		}

		List<ProducerCacheKey<String>> expectedCacheKeys = new ArrayList<>();
		expectedCacheKeys.add(new ProducerCacheKey<>(schema1, topic1, null, null));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema1, topic1, encryptionKeys1, null));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema1, topic1, encryptionKeys1, customizers1));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema1, topic1, encryptionKeys1, customizers2));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema1, topic1, encryptionKeys2, null));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema1, topic1, encryptionKeys2, customizers1));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema1, topic1, encryptionKeys2, customizers2));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema2, topic1, null, null));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema2, topic1, encryptionKeys1, null));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema2, topic1, encryptionKeys1, customizers1));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema2, topic1, encryptionKeys1, customizers2));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema2, topic1, encryptionKeys2, null));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema2, topic1, encryptionKeys2, customizers1));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema2, topic1, encryptionKeys2, customizers2));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema1, topic2, null, null));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema1, topic2, encryptionKeys1, null));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema1, topic2, encryptionKeys1, customizers1));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema1, topic2, encryptionKeys1, customizers2));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema1, topic2, encryptionKeys2, null));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema1, topic2, encryptionKeys2, customizers1));
		expectedCacheKeys.add(new ProducerCacheKey<>(schema1, topic2, encryptionKeys2, customizers2));

		getAssertedProducerCache(producerFactory, expectedCacheKeys);
	}

	@Test
	void factoryDestroyCleansUpCacheAndClosesProducers() throws PulsarClientException {
		CachingPulsarProducerFactory<String> producerFactory = producerFactory(pulsarClient, Collections.emptyMap());
		var actualProducer1 = actualProducer(producerFactory.createProducer(schema, "topic1"));
		var actualProducer2 = actualProducer(producerFactory.createProducer(schema, "topic2"));
		var cacheKey1 = new ProducerCacheKey<>(schema, "topic1", null, null);
		var cacheKey2 = new ProducerCacheKey<>(schema, "topic2", null, null);
		var producerCache = getAssertedProducerCache(producerFactory, Arrays.asList(cacheKey1, cacheKey2));
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
		var actualProducer = actualProducer(producerFactory.createProducer(schema, "topic1"));
		var cacheKey = new ProducerCacheKey<>(schema, "topic1", null, null);
		var producerCache = getAssertedProducerCache(producerFactory, Collections.singletonList(cacheKey));
		Awaitility.await().pollDelay(Duration.ofSeconds(5L)).timeout(Duration.ofSeconds(10L)).untilAsserted(() -> {
			assertThat(producerCache.asMap()).isEmpty();
			assertThat(actualProducer.isConnected()).isFalse();
		});
	}

	@Test
	void createProducerEncountersException() {
		pulsarClient = spy(pulsarClient);
		when(this.pulsarClient.newProducer(schema)).thenThrow(new RuntimeException("5150"));
		var producerFactory = producerFactory(pulsarClient, Collections.emptyMap());
		assertThatThrownBy(() -> producerFactory.createProducer(schema, "topic1")).isInstanceOf(RuntimeException.class)
				.hasMessage("5150");
		getAssertedProducerCache(producerFactory, Collections.emptyList());
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

	@SuppressWarnings("unchecked")
	@Override
	protected Producer<String> actualProducer(Producer<String> wrappedProducer) {
		assertThat(wrappedProducer).isInstanceOf(ProducerWithCloseCallback.class);
		return ((ProducerWithCloseCallback) wrappedProducer).getActualProducer();
	}

	@Override
	protected CachingPulsarProducerFactory<String> producerFactory(PulsarClient pulsarClient,
			Map<String, Object> producerConfig) {
		var producerFactory = new CachingPulsarProducerFactory<String>(pulsarClient, producerConfig,
				Duration.ofMinutes(5L), 30L, 2);
		producerFactories.add(producerFactory);
		return producerFactory;
	}

	@Nested
	class ProducerCacheKeyTests {

		@Test
		void nullSchemaIsNotAllowed() {
			assertThatThrownBy(() -> new ProducerCacheKey<>(null, "topic1", null, null))
					.isInstanceOf(IllegalArgumentException.class).hasMessage("'schema' must be non-null");
		}

		@Test
		void nullTopicIsNotAllowed() {
			assertThatThrownBy(() -> new ProducerCacheKey<>(schema, null, null, null))
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
			var encryptionKeys1 = Set.of("key1");
			List<ProducerBuilderCustomizer<String>> customizers1 = List.of(p -> p.property("key", "value"));
			var key1 = new ProducerCacheKey<>(Schema.STRING, "topic1", encryptionKeys1, customizers1);
			return Stream
					.of(arguments(Named.of("differentClass", key1), "someStrangeObject", false),
							arguments(Named.of("null", key1), null, false),
							arguments(Named.of("sameInstance", key1), key1, true),
							arguments(Named.of("sameSchemaSameTopicSameNullEncryptionKeysSameNullCustomizers",
									new ProducerCacheKey<>(Schema.STRING, "topic1", null, null)),
									new ProducerCacheKey<>(Schema.STRING, "topic1", null, null), true),
							arguments(
									Named.of("sameSchemaSameTopicSameNonNullEncryptionKeysSameNullCustomizers",
											new ProducerCacheKey<>(Schema.STRING, "topic1", encryptionKeys1, null)),
									new ProducerCacheKey<>(Schema.STRING, "topic1", encryptionKeys1, null), true),
							arguments(
									Named.of("differentSchemaInstanceSameSchemaType",
											new ProducerCacheKey<>(new StringSchema(), "topic1", encryptionKeys1,
													null)),
									new ProducerCacheKey<>(new StringSchema(), "topic1", encryptionKeys1, null), true),
							arguments(
									Named.of("differentSchemaType",
											new ProducerCacheKey<>(Schema.STRING, "topic1", encryptionKeys1, null)),
									new ProducerCacheKey<>(Schema.INT64, "topic1", encryptionKeys1, null), false),
							arguments(
									Named.of("differentTopic",
											new ProducerCacheKey<>(Schema.STRING, "topic1", encryptionKeys1,
													customizers1)),
									new ProducerCacheKey<>(Schema.STRING, "topic2", encryptionKeys1, customizers1),
									false),
							arguments(
									Named.of("differentNonNullEncryptionKeys",
											new ProducerCacheKey<>(Schema.STRING, "topic1", encryptionKeys1, null)),
									new ProducerCacheKey<>(Schema.STRING, "topic1", Set.of("key2"), null), false),
							arguments(
									Named.of("differentNullEncryptionKeys",
											new ProducerCacheKey<>(Schema.STRING, "topic1", encryptionKeys1, null)),
									new ProducerCacheKey<>(Schema.STRING, "topic1", null, null), false),
							arguments(
									Named.of("differentNonNullCustomizers",
											new ProducerCacheKey<>(Schema.STRING, "topic1", encryptionKeys1,
													customizers1)),
									new ProducerCacheKey<>(Schema.STRING, "topic1", encryptionKeys1,
											List.of(p -> p.property("key", "value"))),
									false),
							arguments(
									Named.of("differentNullInterceptor",
											new ProducerCacheKey<>(Schema.STRING, "topic1", encryptionKeys1,
													customizers1)),
									new ProducerCacheKey<>(Schema.STRING, "topic1", null, null), false));
		}

	}

}
