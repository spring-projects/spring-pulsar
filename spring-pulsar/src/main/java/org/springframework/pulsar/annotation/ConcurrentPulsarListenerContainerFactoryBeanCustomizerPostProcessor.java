/*
 * Copyright 2023-2024 the original author or authors.
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

package org.springframework.pulsar.annotation;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.config.ConcurrentPulsarListenerContainerFactory;
import org.springframework.pulsar.config.ConcurrentPulsarListenerContainerFactoryCustomizer;
import org.springframework.util.CollectionUtils;

/**
 * Applies a {@link ConcurrentPulsarListenerContainerFactoryCustomizer} to all
 * {@link ConcurrentPulsarListenerContainerFactory} beans.
 * <p>
 * There must be only one customizer in the application context in order for it to be
 * applied.
 *
 * @author Chris Bono
 */
class ConcurrentPulsarListenerContainerFactoryBeanCustomizerPostProcessor
		implements BeanPostProcessor, ApplicationContextAware {

	private final LogAccessor logger = new LogAccessor(getClass());

	private ApplicationContext applicationContext;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
		if (bean instanceof ConcurrentPulsarListenerContainerFactory<?> containerFactory) {
			var customizers = this.applicationContext
				.getBeansOfType(ConcurrentPulsarListenerContainerFactoryCustomizer.class);
			if (CollectionUtils.isEmpty(customizers)) {
				return bean;
			}
			if (customizers.size() > 1) {
				this.logger.warn("Found multiple %s beans [%s] - must be only 1 in order to apply".formatted(
						ConcurrentPulsarListenerContainerFactoryCustomizer.class.getSimpleName(),
						customizers.keySet()));
			}
			else {
				customizers.values().forEach((c) -> c.customize(containerFactory));
			}
		}
		return bean;
	}

}
