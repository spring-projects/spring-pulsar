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

package org.springframework.pulsar.core;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

/**
 * Topic resolver that accepts custom type to topic mappings and uses the mappings during
 * topic resolution.
 * <p>
 * Message type to topic mappings can be configured with
 * {@link #addCustomTopicMapping(Class, String)}.
 *
 * @author Chris Bono
 */
public class DefaultTopicResolver implements TopicResolver {

	private final Map<Class<?>, String> customTopicMappings = new LinkedHashMap<>();

	/**
	 * Adds a custom mapping from message type to topic.
	 * @param messageType the message type
	 * @param topic the topic to use for messages of type {@code messageType}
	 * @return the previously mapped topic or {@code null} if there was no mapping for
	 * {@code messageType}.
	 */
	@Nullable
	public String addCustomTopicMapping(Class<?> messageType, String topic) {
		return this.customTopicMappings.put(messageType, topic);
	}

	/**
	 * Removes the custom mapping from message type to topic.
	 * @param messageType the message type
	 * @return the previously mapped topic or {@code null} if there was no mapping for
	 * {@code messageType}.
	 */
	@Nullable
	public String removeCustomMapping(Class<?> messageType) {
		return this.customTopicMappings.remove(messageType);
	}

	/**
	 * Gets the currently registered custom mappings from message type to topic.
	 * @return unmodifiable map of custom mappings
	 */
	public Map<Class<?>, String> getCustomTopicMappings() {
		return Collections.unmodifiableMap(this.customTopicMappings);
	}

	@Override
	public Resolved<String> resolveTopic(@Nullable String userSpecifiedTopic, Supplier<String> defaultTopicSupplier) {
		if (StringUtils.hasText(userSpecifiedTopic)) {
			return Resolved.of(userSpecifiedTopic);
		}
		String defaultTopic = defaultTopicSupplier.get();
		if (defaultTopic == null) {
			return Resolved.failed("Topic must be specified when no default topic is configured");
		}
		return Resolved.of(defaultTopic);
	}

	@Override
	public <T> Resolved<String> resolveTopic(@Nullable String userSpecifiedTopic, @Nullable T message,
			Supplier<String> defaultTopicSupplier) {
		return doResolveTopic(userSpecifiedTopic, message != null ? message.getClass() : null, defaultTopicSupplier);
	}

	@Override
	public Resolved<String> resolveTopic(@Nullable String userSpecifiedTopic, @Nullable Class<?> messageType,
			Supplier<String> defaultTopicSupplier) {
		return doResolveTopic(userSpecifiedTopic, messageType, defaultTopicSupplier);
	}

	protected Resolved<String> doResolveTopic(@Nullable String userSpecifiedTopic, @Nullable Class<?> messageType,
			Supplier<String> defaultTopicSupplier) {
		if (StringUtils.hasText(userSpecifiedTopic)) {
			return Resolved.of(userSpecifiedTopic);
		}
		if (messageType == null) {
			return Resolved.failed("Topic must be specified when the message is null");
		}
		String topic = this.customTopicMappings.getOrDefault(messageType, defaultTopicSupplier.get());
		return topic == null ? Resolved.failed("Topic must be specified when no default topic is configured")
				: Resolved.of(topic);
	}

}
