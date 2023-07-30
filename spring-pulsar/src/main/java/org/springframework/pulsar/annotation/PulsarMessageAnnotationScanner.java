/*
 * Copyright 2023 the original author or authors.
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

import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.pulsar.config.PulsarAnnotationSupportBeanNames;
import org.springframework.pulsar.config.PulsarMessageRegistrar;
import org.springframework.util.ClassUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Scans classpath for {@code @PulsarMessage} annotated classes, creates descriptors for
 * them and registers {@code PulsarMessageRegistrar} bean to add default topic and schema
 * mappings based on collected descriptors.
 *
 * @author Aleksei Arsenev
 */
public class PulsarMessageAnnotationScanner implements ImportBeanDefinitionRegistrar {

	@Override
	public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
		ClassPathScanningCandidateComponentProvider componentProvider = new ClassPathScanningCandidateComponentProvider(
				false);
		componentProvider.addIncludeFilter(new AnnotationTypeFilter(PulsarMessage.class));
		List<PulsarMessageRegistrar.MessageDescriptor> messageDescriptors = new ArrayList<>();
		for (BeanDefinition candidate : componentProvider
				.findCandidateComponents(ClassUtils.getPackageName(importingClassMetadata.getClassName()))) {
			if (candidate instanceof AnnotatedBeanDefinition) {
				AnnotationMetadata metadata = ((AnnotatedBeanDefinition) candidate).getMetadata();
				var annotation = metadata.getAnnotations().get(PulsarMessage.class);

				Class<?> clazz;
				try {
					clazz = ClassUtils.forName(candidate.getBeanClassName(), ClassUtils.getDefaultClassLoader());
				}
				catch (ClassNotFoundException e) {
					throw new IllegalStateException(e);
				}
				var descriptor = new PulsarMessageRegistrar.MessageDescriptor(clazz, annotation);
				messageDescriptors.add(descriptor);
			}
		}

		GenericBeanDefinition definition = new GenericBeanDefinition();
		definition.setBeanClass(PulsarMessageRegistrar.class);
		definition.getPropertyValues().addPropertyValue("descriptors", messageDescriptors);
		definition.setDependencyCheck(AbstractBeanDefinition.DEPENDENCY_CHECK_ALL);
		registry.registerBeanDefinition(PulsarAnnotationSupportBeanNames.PULSAR_MESSAGE_REGISTRAR_BEAN_NAME,
				definition);
	}

}
