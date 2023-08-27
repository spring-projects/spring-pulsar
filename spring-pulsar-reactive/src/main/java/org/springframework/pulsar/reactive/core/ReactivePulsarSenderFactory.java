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

package org.springframework.pulsar.reactive.core;

import java.util.List;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.api.ReactiveMessageSender;

import org.springframework.lang.Nullable;

/**
 * The strategy to create a {@link ReactiveMessageSender} instance(s).
 *
 * @param <T> reactive message sender payload type
 * @author Christophe Bornet
 * @author Chris Bono
 */
public interface ReactivePulsarSenderFactory<T> {

	/**
	 * Create a reactive message sender.
	 * @param topic the topic to send messages to or {@code null} to use the default topic
	 * @param schema the schema of the messages to be sent
	 * @return the reactive message sender
	 */
	ReactiveMessageSender<T> createSender(Schema<T> schema, @Nullable String topic);

	/**
	 * Create a reactive message sender.
	 * @param schema the schema of the messages to be sent
	 * @param topic the topic to send messages to or {@code null} to use the default topic
	 * @param customizer the optional customizer to apply to the reactive message sender
	 * builder
	 * @return the reactive message sender
	 */
	ReactiveMessageSender<T> createSender(Schema<T> schema, @Nullable String topic,
			@Nullable ReactiveMessageSenderBuilderCustomizer<T> customizer);

	/**
	 * Create a reactive message sender.
	 * @param schema the schema of the messages to be sent
	 * @param topic the topic to send messages to or {@code null} to use the default topic
	 * @param customizers the optional list of customizers to apply to the reactive
	 * message sender builder
	 * @return the reactive message sender
	 */
	ReactiveMessageSender<T> createSender(Schema<T> schema, @Nullable String topic,
			@Nullable List<ReactiveMessageSenderBuilderCustomizer<T>> customizers);

	/**
	 * Get the default topic to use for all created senders.
	 * @return the default topic to use for all created senders or null if no default set.
	 */
	@Nullable
	String getDefaultTopic();

}
