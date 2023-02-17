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

package org.springframework.pulsar.reactive.config.annotation;

import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.pulsar.config.PulsarAnnotationSupportBeanNames;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerEndpointRegistry;

/**
 * An {@link ImportBeanDefinitionRegistrar} class that registers a
 * {@link ReactivePulsarListenerAnnotationBeanPostProcessor} bean capable of processing
 * Spring's @{@link ReactivePulsarListener} annotation. Also register a default
 * {@link ReactivePulsarListenerEndpointRegistry}.
 *
 * <p>
 * This configuration class is automatically imported when using
 * the @{@link EnableReactivePulsar} annotation.
 *
 * @author Christophe Bornet
 * @see ReactivePulsarListenerAnnotationBeanPostProcessor
 * @see ReactivePulsarListenerEndpointRegistry
 * @see EnableReactivePulsar
 */
public class ReactivePulsarBootstrapConfiguration implements ImportBeanDefinitionRegistrar {

	@Override
	public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
		if (!registry.containsBeanDefinition(
				PulsarAnnotationSupportBeanNames.REACTIVE_PULSAR_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME)) {
			registry.registerBeanDefinition(
					PulsarAnnotationSupportBeanNames.REACTIVE_PULSAR_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME,
					new RootBeanDefinition(ReactivePulsarListenerAnnotationBeanPostProcessor.class));
		}

		if (!registry.containsBeanDefinition(
				PulsarAnnotationSupportBeanNames.REACTIVE_PULSAR_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME)) {
			registry.registerBeanDefinition(
					PulsarAnnotationSupportBeanNames.REACTIVE_PULSAR_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME,
					new RootBeanDefinition(ReactivePulsarListenerEndpointRegistry.class));
		}
	}

}
