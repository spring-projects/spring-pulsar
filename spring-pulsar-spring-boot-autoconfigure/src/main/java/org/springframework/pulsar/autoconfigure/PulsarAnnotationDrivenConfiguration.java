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

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.pulsar.config.ConcurrentPulsarListenerContainerFactory;
import org.springframework.pulsar.config.PulsarListenerBeanNames;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.listener.PulsarContainerProperties;
import org.springframework.pulsar.observation.PulsarListenerObservationConvention;
import org.springframework.util.unit.DataSize;

import io.micrometer.observation.ObservationRegistry;

/**
 * Configuration for Pulsar annotation-driven support.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(EnablePulsar.class)
public class PulsarAnnotationDrivenConfiguration {

	private final PulsarProperties pulsarProperties;

	public PulsarAnnotationDrivenConfiguration(PulsarProperties pulsarProperties) {
		this.pulsarProperties = pulsarProperties;
	}

	@Bean
	@ConditionalOnMissingBean(name = "pulsarListenerContainerFactory")
	ConcurrentPulsarListenerContainerFactory<?> pulsarListenerContainerFactory(
			ObjectProvider<PulsarConsumerFactory<Object>> consumerFactoryProvider,
			ObjectProvider<ObservationRegistry> observationRegistryProvider,
			ObjectProvider<PulsarListenerObservationConvention> observationConventionProvider) {

		PulsarContainerProperties containerProperties = new PulsarContainerProperties();
		containerProperties.setSubscriptionType(this.pulsarProperties.getConsumer().getSubscriptionType());
		containerProperties.setObservationConvention(observationConventionProvider.getIfUnique());

		PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();
		PulsarProperties.Listener listenerProperties = this.pulsarProperties.getListener();
		map.from(listenerProperties::getSchemaType).to(containerProperties::setSchemaType);
		map.from(listenerProperties::getAckMode).to(containerProperties::setAckMode);
		map.from(listenerProperties::getBatchTimeout).asInt(Duration::toMillis)
				.to(containerProperties::setBatchTimeoutMillis);
		map.from(listenerProperties::getMaxNumBytes).asInt(DataSize::toBytes).to(containerProperties::setMaxNumBytes);
		map.from(listenerProperties::getMaxNumMessages).to(containerProperties::setMaxNumMessages);

		return new ConcurrentPulsarListenerContainerFactory<>(consumerFactoryProvider.getIfAvailable(),
				containerProperties, this.pulsarProperties.getListener().isObservationsEnabled()
						? observationRegistryProvider.getIfUnique() : null);
	}

	@Configuration(proxyBeanMethods = false)
	@EnablePulsar
	@ConditionalOnMissingBean(name = PulsarListenerBeanNames.PULSAR_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME)
	static class EnablePulsarConfiguration {

	}

}
