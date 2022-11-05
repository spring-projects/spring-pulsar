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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;

/**
 * Utility methods to help load configuration into a {@link ConsumerBuilder}.
 * <p>
 * The main purpose is to work around the underlying
 * <a href="https://github.com/apache/pulsar/issues/11646">Pulsar issue</a> where
 * {@code ConsumerBuilder::loadConf} sets {@code @JsonIgnore} fields to null and crashes
 * if a {@code deadLetterPolicy} was set on the builder.
 * <p>
 * Should be removed once the above issue is fixed.
 *
 * @author Chris Bono
 */
public final class ConsumerBuilderConfigurationUtil {

	private ConsumerBuilderConfigurationUtil() {
	}

	/**
	 * Configures the specified properties onto the specified builder in a manner that
	 * loads non-serializable properties. See
	 * <a href="https://github.com/apache/pulsar/pull/18344">Pulsar PR</a>.
	 * @param builder the builder
	 * @param properties the properties to set on the builder
	 * @param <T> the payload type
	 */
	public static <T> void loadConf(ConsumerBuilder<T> builder, Map<String, Object> properties) {

		ConsumerConfigurationData<T> builderConf = ((ConsumerBuilderImpl<T>) builder).getConf();
		Map<String, Object> propertiesCopy = new HashMap<>(properties);

		builder.loadConf(propertiesCopy);

		// Manually set fields marked as @JsonIgnore in ConsumerConfigurationData
		applyValueToBuilderAfterLoadConf(builderConf::getNegativeAckRedeliveryBackoff,
				builder::negativeAckRedeliveryBackoff, propertiesCopy, "negativeAckRedeliveryBackoff");
		applyValueToBuilderAfterLoadConf(builderConf::getAckTimeoutRedeliveryBackoff,
				builder::ackTimeoutRedeliveryBackoff, propertiesCopy, "ackTimeoutRedeliveryBackoff");
	}

	@SuppressWarnings("unchecked")
	private static <T> void applyValueToBuilderAfterLoadConf(Supplier<T> confGetter, Consumer<T> builderSetter,
			Map<String, Object> properties, String propertyName) {
		T value = (T) properties.getOrDefault(propertyName, confGetter.get());

		if (value != null) {
			builderSetter.accept(value);
		}
	}

}
