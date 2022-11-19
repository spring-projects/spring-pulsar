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
 * <a href="https://github.com/apache/pulsar/pull/18344">Pulsar issue</a> where
 * {@link ConsumerBuilder#loadConf} sets {@code @JsonIgnore} fields to null.
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
		propertiesCopy.remove("messageListener");
		propertiesCopy.remove("consumerEventListener");
		propertiesCopy.remove("negativeAckRedeliveryBackoff");
		propertiesCopy.remove("ackTimeoutRedeliveryBackoff");
		propertiesCopy.remove("cryptoKeyReader");
		propertiesCopy.remove("messageCrypto");
		propertiesCopy.remove("batchReceivePolicy");
		propertiesCopy.remove("keySharedPolicy");
		propertiesCopy.remove("payloadProcessor");

		builder.loadConf(propertiesCopy);

		// Manually set fields marked as @JsonIgnore in ConsumerConfigurationData
		applyValueToBuilderAfterLoadConf(builderConf::getMessageListener, builder::messageListener, properties,
				"messageListener");
		applyValueToBuilderAfterLoadConf(builderConf::getConsumerEventListener, builder::consumerEventListener,
				properties, "consumerEventListener");
		applyValueToBuilderAfterLoadConf(builderConf::getNegativeAckRedeliveryBackoff,
				builder::negativeAckRedeliveryBackoff, properties, "negativeAckRedeliveryBackoff");
		applyValueToBuilderAfterLoadConf(builderConf::getAckTimeoutRedeliveryBackoff,
				builder::ackTimeoutRedeliveryBackoff, properties, "ackTimeoutRedeliveryBackoff");
		applyValueToBuilderAfterLoadConf(builderConf::getCryptoKeyReader, builder::cryptoKeyReader, properties,
				"cryptoKeyReader");
		applyValueToBuilderAfterLoadConf(builderConf::getMessageCrypto, builder::messageCrypto, properties,
				"messageCrypto");
		applyValueToBuilderAfterLoadConf(builderConf::getBatchReceivePolicy, builder::batchReceivePolicy, properties,
				"batchReceivePolicy");
		applyValueToBuilderAfterLoadConf(builderConf::getKeySharedPolicy, builder::keySharedPolicy, properties,
				"keySharedPolicy");
		applyValueToBuilderAfterLoadConf(builderConf::getPayloadProcessor, builder::messagePayloadProcessor, properties,
				"payloadProcessor");
	}

	@SuppressWarnings("unchecked")
	private static <T> void applyValueToBuilderAfterLoadConf(Supplier<T> confGetter, Consumer<T> builderSetter,
			Map<String, Object> properties, String key) {
		T value = (T) properties.getOrDefault(key, confGetter.get());
		if (value != null) {
			builderSetter.accept(value);
		}
	}

}
