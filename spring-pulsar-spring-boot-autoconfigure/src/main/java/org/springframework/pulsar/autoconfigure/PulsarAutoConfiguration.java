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

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.pulsar.config.PulsarClientConfiguration;
import org.springframework.pulsar.config.PulsarClientFactoryBean;
import org.springframework.pulsar.core.CachingPulsarProducerFactory;
import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.core.PulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.SchemaResolver;
import org.springframework.pulsar.function.PulsarFunction;
import org.springframework.pulsar.function.PulsarFunctionAdministration;
import org.springframework.pulsar.function.PulsarSink;
import org.springframework.pulsar.function.PulsarSource;
import org.springframework.pulsar.observation.PulsarTemplateObservationConvention;

import io.micrometer.observation.ObservationRegistry;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for Apache Pulsar.
 *
 * @author Soby Chacko
 * @author Chris Bono
 * @author Alexander Preuß
 */
@AutoConfiguration
@ConditionalOnClass(PulsarTemplate.class)
@EnableConfigurationProperties(PulsarProperties.class)
@Import({ PulsarAnnotationDrivenConfiguration.class })
public class PulsarAutoConfiguration {

	private final PulsarProperties properties;

	public PulsarAutoConfiguration(PulsarProperties properties) {
		this.properties = properties;
	}

	@Bean
	@ConditionalOnMissingBean
	public PulsarClientConfiguration pulsarClientConfiguration() {
		return new PulsarClientConfiguration(this.properties.buildClientProperties());
	}

	@Bean
	@ConditionalOnMissingBean
	public PulsarClientFactoryBean pulsarClientFactoryBean(PulsarClientConfiguration pulsarClientConfiguration) {
		return new PulsarClientFactoryBean(pulsarClientConfiguration);
	}

	@Bean
	@ConditionalOnMissingBean
	@ConditionalOnProperty(name = "spring.pulsar.producer.cache.enabled", havingValue = "false")
	public PulsarProducerFactory<?> pulsarProducerFactory(PulsarClient pulsarClient) {
		return new DefaultPulsarProducerFactory<>(pulsarClient, this.properties.buildProducerProperties());
	}

	@Bean
	@ConditionalOnMissingBean
	@ConditionalOnProperty(name = "spring.pulsar.producer.cache.enabled", havingValue = "true", matchIfMissing = true)
	public PulsarProducerFactory<?> cachingPulsarProducerFactory(PulsarClient pulsarClient) {
		return new CachingPulsarProducerFactory<>(pulsarClient, this.properties.buildProducerProperties(),
				this.properties.getProducer().getCache().getExpireAfterAccess(),
				this.properties.getProducer().getCache().getMaximumSize(),
				this.properties.getProducer().getCache().getInitialCapacity());
	}

	@Bean
	@ConditionalOnMissingBean
	public PulsarTemplate<?> pulsarTemplate(PulsarProducerFactory<?> pulsarProducerFactory,
			ObjectProvider<ProducerInterceptor> interceptorsProvider, SchemaResolver schemaResolver,
			ObjectProvider<ObservationRegistry> observationRegistryProvider,
			ObjectProvider<PulsarTemplateObservationConvention> observationConventionProvider) {
		return new PulsarTemplate<>(pulsarProducerFactory, interceptorsProvider.orderedStream().toList(),
				schemaResolver, this.properties.getTemplate().isObservationsEnabled()
						? observationRegistryProvider.getIfUnique() : null,
				observationConventionProvider.getIfUnique());
	}

	@Bean
	@ConditionalOnMissingBean
	public SchemaResolver schemaResolver() {
		return new DefaultSchemaResolver();
	}

	@Bean
	@ConditionalOnMissingBean
	public PulsarConsumerFactory<?> pulsarConsumerFactory(PulsarClient pulsarClient) {
		return new DefaultPulsarConsumerFactory<>(pulsarClient, this.properties.buildConsumerProperties());
	}

	@Bean
	@ConditionalOnMissingBean
	public PulsarAdministration pulsarAdministration() {
		return new PulsarAdministration(this.properties.buildAdminProperties());
	}

	@Bean
	@ConditionalOnMissingBean
	@ConditionalOnProperty(name = "spring.pulsar.function.enabled", havingValue = "true", matchIfMissing = true)
	public PulsarFunctionAdministration pulsarFunctionAdministration(PulsarAdministration pulsarAdministration,
			ObjectProvider<PulsarFunction> pulsarFunctions, ObjectProvider<PulsarSink> pulsarSinks,
			ObjectProvider<PulsarSource> pulsarSources) {
		return new PulsarFunctionAdministration(pulsarAdministration, pulsarFunctions, pulsarSinks, pulsarSources,
				this.properties.getFunction().getFailFast(), this.properties.getFunction().getPropagateFailures(),
				this.properties.getFunction().getPropagateStopFailures());
	}

}
