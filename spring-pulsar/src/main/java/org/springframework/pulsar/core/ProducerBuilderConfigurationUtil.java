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

package org.springframework.pulsar.core;

import java.util.Collection;
import java.util.Map;

import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.ProducerBuilder;

/**
 * Utility methods to help load configuration into a {@link ProducerBuilder}.
 * <p>
 * The main purpose is to work around the underlying
 * <a href="https://github.com/apache/pulsar/pull/18344">Pulsar issue</a> where
 * {@link ProducerBuilder#loadConf} sets {@code @JsonIgnore} fields to null.
 * <p>
 * Should be removed once the above issue is fixed.
 *
 * @author Chris Bono
 * @deprecated since 1.1.0 for removal in 1.2.0
 */
@Deprecated(since = "1.1.0", forRemoval = true)
public final class ProducerBuilderConfigurationUtil {

	private ProducerBuilderConfigurationUtil() {
	}

	/**
	 * Configures the specified properties onto the specified builder in a manner that
	 * loads non-serializable properties. See
	 * <a href="https://github.com/apache/pulsar/pull/18344">Pulsar PR</a>.
	 * @param builder the builder
	 * @param properties the properties to set on the builder
	 * @param <T> the payload type
	 */
	public static <T> void loadConf(ProducerBuilder<T> builder, Map<String, Object> properties) {

		builder.loadConf(properties);

		// Set fields that are not loaded by loadConf
		if (properties.containsKey("encryptionKeys")) {
			@SuppressWarnings("unchecked")
			Collection<String> keys = (Collection<String>) properties.get("encryptionKeys");
			keys.forEach(builder::addEncryptionKey);
		}
		if (properties.containsKey("customMessageRouter")) {
			builder.messageRouter((MessageRouter) properties.get("customMessageRouter"));
		}
		if (properties.containsKey("batcherBuilder")) {
			builder.batcherBuilder((BatcherBuilder) properties.get("batcherBuilder"));
		}
		if (properties.containsKey("cryptoKeyReader")) {
			builder.cryptoKeyReader((CryptoKeyReader) properties.get("cryptoKeyReader"));
		}
	}

}
