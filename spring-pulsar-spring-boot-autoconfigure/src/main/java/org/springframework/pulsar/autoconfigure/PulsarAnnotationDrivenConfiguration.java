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
			ObjectProvider<PulsarConsumerFactory<Object>> pulsarConsumerFactory) {
		ConcurrentPulsarListenerContainerFactory<Object> factory = new ConcurrentPulsarListenerContainerFactory<>();

		final PulsarConsumerFactory<Object> pulsarConsumerFactory1 = pulsarConsumerFactory.getIfAvailable();
		factory.setPulsarConsumerFactory(pulsarConsumerFactory1);

		final PulsarContainerProperties containerProperties = factory.getContainerProperties();

		PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();
		PulsarProperties.Listener properties = this.pulsarProperties.getListener();

		map.from(properties::getSchemaType).to(containerProperties::setSchemaType);
		map.from(properties::getAckMode).to(containerProperties::setAckMode);

		return factory;
	}

	@Configuration(proxyBeanMethods = false)
	@EnablePulsar
	@ConditionalOnMissingBean(name = PulsarListenerBeanNames.PULSAR_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME)
	static class EnablePulsarConfiguration {

	}

}
