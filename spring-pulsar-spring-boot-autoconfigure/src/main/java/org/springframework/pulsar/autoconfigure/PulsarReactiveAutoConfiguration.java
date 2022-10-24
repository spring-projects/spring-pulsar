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

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.adapter.ProducerCacheProvider;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSenderCache;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;
import org.apache.pulsar.reactive.client.producercache.CaffeineProducerCacheProvider;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.pulsar.core.reactive.DefaultReactivePulsarSenderFactory;
import org.springframework.pulsar.core.reactive.ReactivePulsarSenderFactory;
import org.springframework.pulsar.core.reactive.ReactivePulsarSenderTemplate;

import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for Apache Pulsar.
 *
 * @author Chris Bono
 * @author Christophe Bornet
 */
@AutoConfiguration(after = PulsarAutoConfiguration.class)
@ConditionalOnClass(ReactivePulsarSenderTemplate.class)
@EnableConfigurationProperties(PulsarReactiveProperties.class)
public class PulsarReactiveAutoConfiguration {

	private final PulsarReactiveProperties properties;

	public PulsarReactiveAutoConfiguration(PulsarReactiveProperties properties) {
		this.properties = properties;
	}

	@Bean
	@ConditionalOnMissingBean(ReactivePulsarClient.class)
	public ReactivePulsarClient pulsarReactivePulsarClient(PulsarClient pulsarClient) {
		return AdaptedReactivePulsarClientFactory.create(pulsarClient);
	}

	@Bean
	@ConditionalOnMissingBean(ProducerCacheProvider.class)
	@ConditionalOnClass(CaffeineProducerCacheProvider.class)
	@ConditionalOnProperty(name = "spring.pulsar.reactive.sender.cache.enabled", havingValue = "true",
			matchIfMissing = true)
	public ProducerCacheProvider pulsarProducerCacheProvider() {
		PulsarReactiveProperties.Cache cache = this.properties.getSender().getCache();
		Caffeine<Object, Object> caffeine = Caffeine.newBuilder().expireAfterAccess(cache.getExpireAfterAccess())
				.maximumSize(cache.getMaximumSize()).initialCapacity(cache.getInitialCapacity());
		return new CaffeineProducerCacheProvider(caffeine);
	}

	@Bean
	@ConditionalOnMissingBean(ReactiveMessageSenderCache.class)
	@ConditionalOnProperty(name = "spring.pulsar.reactive.sender.cache.enabled", havingValue = "true",
			matchIfMissing = true)
	public ReactiveMessageSenderCache pulsarReactiveMessageSenderCache(
			ObjectProvider<ProducerCacheProvider> producerCacheProvider) {
		return producerCacheProvider.stream().findFirst().map(AdaptedReactivePulsarClientFactory::createCache)
				.orElseGet(AdaptedReactivePulsarClientFactory::createCache);
	}

	@Bean
	@ConditionalOnMissingBean(ReactivePulsarSenderFactory.class)
	public ReactivePulsarSenderFactory<?> reactivePulsarSenderFactory(ReactivePulsarClient pulsarReactivePulsarClient,
			ObjectProvider<ReactiveMessageSenderCache> cache) {
		return new DefaultReactivePulsarSenderFactory<>(pulsarReactivePulsarClient,
				this.properties.buildReactiveMessageSenderSpec(), cache.getIfAvailable());
	}

	@Bean
	@ConditionalOnMissingBean(ReactivePulsarSenderTemplate.class)
	public ReactivePulsarSenderTemplate<?> pulsarReactiveSenderTemplate(
			ReactivePulsarSenderFactory<?> reactivePulsarSenderFactory) {
		return new ReactivePulsarSenderTemplate<>(reactivePulsarSenderFactory);
	}

}
