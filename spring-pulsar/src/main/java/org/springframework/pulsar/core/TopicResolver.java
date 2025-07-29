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

package org.springframework.pulsar.core;

import java.util.function.Supplier;

import org.springframework.lang.Nullable;

/**
 * Resolves topics to use when producing or consuming.
 *
 * @author Chris Bono
 */
public interface TopicResolver {

	/**
	 * Resolve the topic name to use.
	 * @param userSpecifiedTopic the topic specified by the user
	 * @param defaultTopicSupplier supplies the default topic to use (use a supplier that
	 * returns {@code null} to signal no default)
	 * @return the topic to use or {@code empty} if no topic could be resolved
	 */
	Resolved<String> resolveTopic(@Nullable String userSpecifiedTopic, Supplier<String> defaultTopicSupplier);

	/**
	 * Resolve the topic name to use for the given message.
	 * @param <T> the message type
	 * @param userSpecifiedTopic the topic specified by the user
	 * @param message the message instance being produced or consumed
	 * @param defaultTopicSupplier supplies the default topic to use (use a supplier that
	 * returns {@code null} to signal no default)
	 * @return the topic to use or {@code empty} if no topic could be resolved
	 */
	<T> Resolved<String> resolveTopic(@Nullable String userSpecifiedTopic, @Nullable T message,
			Supplier<String> defaultTopicSupplier);

	/**
	 * Resolve the topic name to use for the given message type.
	 * @param userSpecifiedTopic the topic specified by the user
	 * @param messageType the message type being produced or consumed
	 * @param defaultTopicSupplier supplies the default topic to use (use a supplier that
	 * returns {@code null} to signal no default)
	 * @return the topic to use or {@code empty} if no topic could be resolved
	 */
	Resolved<String> resolveTopic(@Nullable String userSpecifiedTopic, @Nullable Class<?> messageType,
			Supplier<String> defaultTopicSupplier);

}
