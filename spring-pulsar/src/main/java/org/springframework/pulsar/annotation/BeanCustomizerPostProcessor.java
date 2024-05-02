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
import org.springframework.util.CollectionUtils;

/**
 * A {@link BeanPostProcessor} that applies a customizer to beans of a specified type.
 * <p>
 * There must be only one customizer in the application context in order for it to be
 * applied.
 *
 * @param <B> the type of bean to customize
 * @param <C> the type of customizer
 * @author Chris Bono
 */
class BeanCustomizerPostProcessor<B, C extends BeanCustomizer<B>>
		implements BeanPostProcessor, ApplicationContextAware {

	private final LogAccessor logger = new LogAccessor(getClass());

	private final Class<B> beanType;

	private final Class<C> customizerType;

	private ApplicationContext applicationContext;

	BeanCustomizerPostProcessor(Class<B> beanType, Class<C> customizerType) {
		this.beanType = beanType;
		this.customizerType = customizerType;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Override
	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
		if (this.beanType.isInstance(bean)) {
			B typedBean = this.beanType.cast(bean);
			var customizers = this.applicationContext.getBeansOfType(this.customizerType);
			if (CollectionUtils.isEmpty(customizers)) {
				return bean;
			}
			if (customizers.size() > 1) {
				this.logger.warn("Found multiple %s beans [%s] - must be only 1 in order to apply"
					.formatted(this.customizerType.getSimpleName(), customizers.keySet()));
			}
			else {
				customizers.values().stream().forEach((c) -> c.customize(typedBean));
			}
		}
		return bean;
	}

}
