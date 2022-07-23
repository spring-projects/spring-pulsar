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

import java.util.Map;

import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

/**
 * The strategy to create a {@link Producer} instance(s).
 *
 * @param <T> producer payload type
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public interface PulsarProducerFactory<T> {

	/**
	 * Create a producer.
	 *
	 * @param topic the topic the producer will send messages to or {@code null} to use the default topic
	 * @param schema the schema of the messages to be sent
	 * @return the producer
	 * @throws PulsarClientException if any error occurs
	 */
	Producer<T> createProducer(String topic, Schema<T> schema) throws PulsarClientException;

	/**
	 * Create a producer.
	 *
	 * @param topic the topic the producer will send messages to or {@code null} to use the default topic
	 * @param schema the schema of the messages to be sent
	 * @param messageRouter the optional message router to use
	 * @return the producer
	 * @throws PulsarClientException if any error occurs
	 */
	Producer<T> createProducer(String topic, Schema<T> schema, MessageRouter messageRouter) throws PulsarClientException;

	/**
	 * Return a map of configuration options to use when creating producers.
	 *
	 * @return the map of configuration options
	 */
	Map<String, Object> getProducerConfig();
}
