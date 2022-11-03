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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import org.springframework.lang.Nullable;

/**
 * Pulsar consumer factory interface.
 *
 * @param <T> payload type for the consumer.
 * @author Soby Chacko
 * @author Christophe Bornet
 */
public interface PulsarConsumerFactory<T> {

	/**
	 * Create a consumer.
	 * @param schema the schema of the messages to be sent
	 * @return the consumer
	 * @throws PulsarClientException if any error occurs
	 */
	Consumer<T> createConsumer(Schema<T> schema) throws PulsarClientException;

	/**
	 * Create a consumer.
	 * @param schema the schema of the messages to be sent
	 * @param topics the topics the consumer will subscribe to builder
	 * @return the consumer
	 * @throws PulsarClientException if any error occurs
	 */
	Consumer<T> createConsumer(Schema<T> schema, Collection<String> topics) throws PulsarClientException;

	/**
	 * Create a consumer.
	 * @param schema the schema of the messages to be sent
	 * @param topics the topics the consumer will subscribe to or {@code null} to use the
	 * default topics
	 * @param properties the properties to attach to the consumer or {@code null} to use
	 * the default properties
	 * @param customizers the optional list of customizers to apply to the consumer
	 * builder
	 * @return the consumer
	 * @throws PulsarClientException if any error occurs
	 */
	Consumer<T> createConsumer(Schema<T> schema, @Nullable Collection<String> topics,
			@Nullable Map<String, String> properties, @Nullable List<ConsumerBuilderCustomizer<T>> customizers)
			throws PulsarClientException;

	/**
	 * Return the configuration options to use when creating consumers.
	 * @return the configuration options
	 */
	Map<String, Object> getConsumerConfig();

}
