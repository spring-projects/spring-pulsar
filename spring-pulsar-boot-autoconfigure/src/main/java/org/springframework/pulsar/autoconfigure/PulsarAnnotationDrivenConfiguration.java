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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.pulsar.config.PulsarListenerConfigUtils;
import org.springframework.pulsar.config.PulsarListenerContainerFactoryImpl;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.listener.PulsarContainerProperties;

/**
 * @author Soby Chacko
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
	PulsarListenerContainerFactoryImpl<?, ?> pulsarListenerContainerFactory(
			ObjectProvider<PulsarConsumerFactory<Object>> pulsarConsumerFactory) {
		PulsarListenerContainerFactoryImpl<Object, Object> factory = new PulsarListenerContainerFactoryImpl<>();

		final PulsarConsumerFactory<Object> pulsarConsumerFactory1 = pulsarConsumerFactory.getIfAvailable();
		factory.setPulsarConsumerFactory(pulsarConsumerFactory1);

		final PulsarContainerProperties containerProperties = factory.getContainerProperties();

//		PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();
//		PulsarProperties.Listener properties = this.pulsarProperties.getListener();

//		map.from(properties::getSchema).as(
//				schema1 -> switch (schema1) {
//					case STRING -> Schema.STRING;
//					case BYTES -> Schema.BYTES;
//					case JSON -> Schema.JSON();
//				}).to(containerProperties::setSchema);

		return factory;
	}

	@Configuration(proxyBeanMethods = false)
	@EnablePulsar
	@ConditionalOnMissingBean(name = PulsarListenerConfigUtils.PULSAR_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME)
	static class EnableKafkaConfiguration {

	}

}
