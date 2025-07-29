/*
 * Copyright 2022-present the original author or authors.
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

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.jspecify.annotations.Nullable;

import org.springframework.pulsar.PulsarException;

/**
 * The strategy to create a {@link Producer} instance(s).
 *
 * @param <T> producer payload type
 * @author Soby Chacko
 * @author Chris Bono
 * @author Alexander Preuß
 * @author Christophe Bornet
 * @author Jonas Geiregat
 */
public interface PulsarProducerFactory<T> {

	/**
	 * Get the Pulsar client that the producer factory uses to create producers.
	 * @return the Pulsar client that the producer factory uses to create producers
	 * @since 1.1.0
	 */
	PulsarClient getPulsarClient();

	/**
	 * Create a producer.
	 * @param schema the schema of the messages to be sent
	 * @param topic the topic the producer will send messages to or {@code null} to use
	 * the default topic
	 * @return the producer
	 * @throws PulsarException if any error occurs
	 */
	Producer<T> createProducer(Schema<T> schema, @Nullable String topic);

	/**
	 * Create a producer.
	 * @param schema the schema of the messages to be sent
	 * @param topic the topic the producer will send messages to or {@code null} to use
	 * the default topic
	 * @param customizer the optional customizer to apply to the producer builder
	 * @return the producer
	 * @throws PulsarException if any error occurs
	 */
	Producer<T> createProducer(Schema<T> schema, @Nullable String topic,
			@Nullable ProducerBuilderCustomizer<T> customizer);

	/**
	 * Create a producer.
	 * @param schema the schema of the messages to be sent
	 * @param topic the topic the producer will send messages to or {@code null} to use
	 * the default topic
	 * @param encryptionKeys the encryption keys used by the producer, replacing the
	 * default encryption keys or {@code null} to use the default encryption keys. Beware
	 * that {@link ProducerBuilder} only has {@link ProducerBuilder#addEncryptionKey} and
	 * doesn't have methods to replace the encryption keys.
	 * @param customizers the optional list of customizers to apply to the producer
	 * builder
	 * @return the producer
	 * @throws PulsarException if any error occurs
	 */
	Producer<T> createProducer(Schema<T> schema, @Nullable String topic, @Nullable Collection<String> encryptionKeys,
			@Nullable List<ProducerBuilderCustomizer<T>> customizers);

	/**
	 * Get the default topic to use for all created producers.
	 * @return the default topic to use for all created producers or null if no default
	 * set
	 */
	@Nullable String getDefaultTopic();

}
