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

package org.springframework.pulsar.core;

import static org.mockito.Mockito.spy;

import java.util.Locale;

import org.apache.pulsar.client.api.Consumer;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.pulsar.listener.DefaultPulsarMessageListenerContainer;
import org.springframework.util.Assert;

public abstract class AbstractContainerBaseTests {

	static final PulsarContainer PULSAR_CONTAINER;

	static {
		final DockerImageName PULSAR_IMAGE = isRunningOnMacM1() ? getMacM1PulsarImage() : getStandardPulsarImage();
		PULSAR_CONTAINER = new PulsarContainer(PULSAR_IMAGE);
		PULSAR_CONTAINER.start();
	}

	protected static String getPulsarBrokerUrl() {
		return PULSAR_CONTAINER.getPulsarBrokerUrl();
	}

	protected static String getHttpServiceUrl() {
		return PULSAR_CONTAINER.getHttpServiceUrl();
	}

	private static boolean isRunningOnMacM1() {
		String osName = System.getProperty("os.name").toLowerCase(Locale.ENGLISH);
		String osArchitecture = System.getProperty("os.arch").toLowerCase(Locale.ENGLISH);
		return osName.contains("mac") && osArchitecture.equals("aarch64");
	}

	private static DockerImageName getStandardPulsarImage() {
		return DockerImageName.parse("apachepulsar/pulsar:2.10.1");
	}

	private static DockerImageName getMacM1PulsarImage() {
		return DockerImageName.parse("kezhenxu94/pulsar").asCompatibleSubstituteFor("apachepulsar/pulsar");
	}

	protected Consumer<?> spyOnConsumer(DefaultPulsarMessageListenerContainer<String> container) {
		Consumer<?> consumer = getPropertyValue(container, "listenerConsumer.consumer", Consumer.class);
		consumer = spy(consumer);
		new DirectFieldAccessor(getPropertyValue(container, "listenerConsumer")).setPropertyValue("consumer", consumer);
		return consumer;
	}

	/**
	 * Uses nested {@link DirectFieldAccessor}s to obtain a property using dotted notation
	 * to traverse fields; e.g. "foo.bar.baz" will obtain a reference to the baz field of
	 * the bar field of foo. Adopted from Spring Integration.
	 * @param root The object.
	 * @param propertyPath The path.
	 * @return The field.
	 */
	public static Object getPropertyValue(Object root, String propertyPath) {
		Object value = null;
		DirectFieldAccessor accessor = new DirectFieldAccessor(root);
		String[] tokens = propertyPath.split("\\.");
		for (int i = 0; i < tokens.length; i++) {
			value = accessor.getPropertyValue(tokens[i]);
			if (value != null) {
				accessor = new DirectFieldAccessor(value);
			}
			else if (i == tokens.length - 1) {
				return null;
			}
			else {
				throw new IllegalArgumentException("intermediate property '" + tokens[i] + "' is null");
			}
		}
		return value;
	}

	@SuppressWarnings("unchecked")
	public static <T> T getPropertyValue(Object root, String propertyPath, Class<T> type) {
		Object value = getPropertyValue(root, propertyPath);
		if (value != null) {
			Assert.isAssignable(type, value.getClass());
		}
		return (T) value;
	}

}
