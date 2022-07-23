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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.pulsar.core.CachingPulsarProducerFactory.SchemaTopic;
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
		SchemaTopic<String> cacheKey = new SchemaTopic<>(schema, "topic1", null);

		Producer<String> producer1 = producerFactory.createProducer("topic1", schema);
		Producer<String> producer2 = producerFactory.createProducer("topic1", schema);
		Producer<String> producer3 = producerFactory.createProducer("topic1", schema);
		assertThat(producer1).isSameAs(producer2).isSameAs(producer3);

		Cache<SchemaTopic<String>, Producer<String>> producerCache = getAssertedProducerCache(producerFactory, Collections.singletonList(cacheKey));
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
		MessageRouter router1 = Mockito.mock(MessageRouter.class);
		MessageRouter router2 = Mockito.mock(MessageRouter.class);

		PulsarProducerFactory<String> producerFactory = producerFactory(pulsarClient, Collections.emptyMap());

		producerFactory.createProducer(topic1, schema1);
		producerFactory.createProducer(topic1, schema1, router1);
		producerFactory.createProducer(topic1, schema1, router2);
		producerFactory.createProducer(topic1, schema2);
		producerFactory.createProducer(topic1, schema2, router1);
		producerFactory.createProducer(topic1, schema2, router2);
		producerFactory.createProducer(topic2, schema1);
		producerFactory.createProducer(topic2, schema1, router1);
		producerFactory.createProducer(topic2, schema1, router2);

		List<SchemaTopic<String>> expectedCacheKeys = new ArrayList<>();
		expectedCacheKeys.add(new SchemaTopic<>(schema1, topic1, null));
		expectedCacheKeys.add(new SchemaTopic<>(schema1, topic1, router1));
		expectedCacheKeys.add(new SchemaTopic<>(schema1, topic1, router2));
		expectedCacheKeys.add(new SchemaTopic<>(schema1, topic2, null));
		expectedCacheKeys.add(new SchemaTopic<>(schema1, topic2, router1));
		expectedCacheKeys.add(new SchemaTopic<>(schema1, topic2, router2));
		expectedCacheKeys.add(new SchemaTopic<>(schema2, topic1, null));
		expectedCacheKeys.add(new SchemaTopic<>(schema2, topic1, router1));
		expectedCacheKeys.add(new SchemaTopic<>(schema2, topic1, router2));

		getAssertedProducerCache(producerFactory, expectedCacheKeys);
	}

	@Test
	void factoryDestroyCleansUpCacheAndClosesProducers() throws PulsarClientException {
		CachingPulsarProducerFactory<String> producerFactory = producerFactory(pulsarClient, Collections.emptyMap());
		SchemaTopic<String> cacheKey1 = new SchemaTopic<>(schema, "topic1", null);
		SchemaTopic<String> cacheKey2 = new SchemaTopic<>(schema, "topic2", null);

		Producer<String> actualProducer1 = actualProducerFrom(producerFactory.createProducer("topic1", schema));
		Producer<String> actualProducer2 = actualProducerFrom(producerFactory.createProducer("topic2", schema));

		Cache<SchemaTopic<String>, Producer<String>> producerCache = getAssertedProducerCache(producerFactory,
				Arrays.asList(cacheKey1, cacheKey2));
		producerFactory.destroy();
		Awaitility.await()
				.timeout(Duration.ofSeconds(5L))
				.untilAsserted(() -> {
					assertThat(producerCache.asMap()).isEmpty();
					assertThat(actualProducer1.isConnected()).isFalse();
					assertThat(actualProducer2.isConnected()).isFalse();
				});
	}

	@Test
	void producerEvictedFromCache() throws PulsarClientException {
		CachingPulsarProducerFactory<String> producerFactory = new CachingPulsarProducerFactory<>(pulsarClient,
				Collections.emptyMap(), Duration.ofSeconds(3L), 10L, 2);
		SchemaTopic<String> cacheKey = new SchemaTopic<>(schema, "topic1", null);

		Producer<String> actualProducer = actualProducerFrom(producerFactory.createProducer("topic1", schema));

		Cache<SchemaTopic<String>, Producer<String>> producerCache = getAssertedProducerCache(producerFactory,
				Collections.singletonList(cacheKey));
		Awaitility.await()
				.pollDelay(Duration.ofSeconds(5L))
				.timeout(Duration.ofSeconds(10L))
				.untilAsserted(() -> {
					assertThat(producerCache.asMap()).isEmpty();
					assertThat(actualProducer.isConnected()).isFalse();
				});
	}

	@Test
	void createProducerEncountersException() {
		pulsarClient = spy(pulsarClient);
		when(this.pulsarClient.newProducer(schema)).thenThrow(new RuntimeException("5150"));
		PulsarProducerFactory<String> producerFactory = producerFactory(pulsarClient, Collections.emptyMap());
		assertThatThrownBy(() -> producerFactory.createProducer("topic1", schema))
				.isInstanceOf(RuntimeException.class)
				.hasMessage("5150");
		getAssertedProducerCache(producerFactory, Collections.emptyList());
	}

	@Override
	protected void assertProducerHasTopicSchemaAndRouter(Producer<String> producer, String topic, Schema<String> schema, MessageRouter router) {
		super.assertProducerHasTopicSchemaAndRouter(actualProducerFrom(producer), topic, schema, router);
	}

	@SuppressWarnings("unchecked")
	private Producer<String> actualProducerFrom(Producer<String> proxyProducer) {
		Producer<String> actualProducer = (Producer<String>) AopProxyUtils.getSingletonTarget(proxyProducer);
		assertThat(actualProducer).isNotNull();
		return actualProducer;
	}

	@SuppressWarnings("unchecked")
	private Cache<SchemaTopic<String>, Producer<String>> getAssertedProducerCache(PulsarProducerFactory<String> producerFactory,
			List<SchemaTopic<String>> expectedCacheKeys) {
		Cache<SchemaTopic<String>, Producer<String>> producerCache = (Cache<SchemaTopic<String>, Producer<String>>)
				ReflectionTestUtils.getField(producerFactory, "producerCache");
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
	protected CachingPulsarProducerFactory<String> producerFactory(PulsarClient pulsarClient, Map<String, Object> producerConfig) {
		CachingPulsarProducerFactory<String> producerFactory = new CachingPulsarProducerFactory<>(pulsarClient,
				producerConfig, Duration.ofMinutes(5L), 10L, 2);
		producerFactories.add(producerFactory);
		return producerFactory;
	}
}
