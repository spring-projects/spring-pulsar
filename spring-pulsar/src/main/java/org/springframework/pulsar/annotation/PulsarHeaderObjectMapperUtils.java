/*
 * Copyright 2023-present the original author or authors.
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

import java.util.Optional;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.core.log.LogAccessor;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Resolves header mapper configuration beans (object mapper and trusted packages) for
 * use during JSON header deserialization.
 *
 * @author Chris Bono
 * @since 1.2.0
 */
public final class PulsarHeaderObjectMapperUtils {

	private static final String PULSAR_HEADER_OBJECT_MAPPER_BEAN_NAME = "pulsarHeaderObjectMapper";

	private static final String PULSAR_HEADER_TRUSTED_PACKAGES_BEAN_NAME = "pulsarHeaderTrustedPackages";

	private static final LogAccessor LOG = new LogAccessor(PulsarHeaderObjectMapperUtils.class);

	private PulsarHeaderObjectMapperUtils() {
	}

	/**
	 * Gets the optional {@link ObjectMapper} to use when deserializing JSON header
	 * values. The mapper bean is expected to be registered with the name
	 * {@code 'pulsarHeaderObjectMapper'}.
	 * @param beanFactory the bean factory that may contain the mapper bean
	 * @return optional mapper or empty if bean not registered under the expected name
	 */
	public static Optional<ObjectMapper> customMapper(BeanFactory beanFactory) {
		Assert.notNull(beanFactory, "beanFactory must not be null");
		try {
			return Optional.of(beanFactory.getBean(PULSAR_HEADER_OBJECT_MAPPER_BEAN_NAME, ObjectMapper.class));
		}
		catch (NoSuchBeanDefinitionException ex) {
			LOG.debug(() -> "No '%s' bean defined - will use standard object mapper for header values"
				.formatted(PULSAR_HEADER_OBJECT_MAPPER_BEAN_NAME));
		}
		return Optional.empty();
	}

	/**
	 * Gets the optional trusted packages to use when deserializing JSON header values.
	 * The bean is expected to be a {@code String[]} registered with the name
	 * {@code 'pulsarHeaderTrustedPackages'}. Trust is by exact package match;
	 * sub-packages must be listed explicitly. Pass {@code "*"} as the sole entry to
	 * trust all packages.
	 * @param beanFactory the bean factory that may contain the trusted packages bean
	 * @return optional trusted packages or empty if bean not registered under the
	 * expected name
	 */
	public static Optional<String[]> trustedPackages(BeanFactory beanFactory) {
		Assert.notNull(beanFactory, "beanFactory must not be null");
		try {
			return Optional.of(beanFactory.getBean(PULSAR_HEADER_TRUSTED_PACKAGES_BEAN_NAME, String[].class));
		}
		catch (NoSuchBeanDefinitionException ex) {
			LOG.debug(() -> "No '%s' bean defined - will use default trusted packages for header deserialization"
				.formatted(PULSAR_HEADER_TRUSTED_PACKAGES_BEAN_NAME));
		}
		return Optional.empty();
	}

}
