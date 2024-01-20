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
import java.util.List;
import java.util.Map;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import org.springframework.lang.Nullable;
import org.springframework.pulsar.PulsarException;

/**
 * Pulsar consumer factory interface.
 *
 * @param <T> payload type for the consumer.
 * @author Soby Chacko
 * @author Christophe Bornet
 * @author Chris Bono
 * @author Jonas Geiregat
 */
public interface PulsarConsumerFactory<T> {

	/**
	 * Create a consumer.
	 * @param schema the schema of the messages to be sent
	 * @param topics the topics the consumer will subscribe to, replacing the default
	 * topics, or {@code null} to use the default topics. Beware that using
	 * {@link ConsumerBuilder#topic} or {@link ConsumerBuilder#topics} will add to the
	 * default topics, not override them. Also beware that specifying {@code null} when no
	 * default topic is configured will result in an exception.
	 * @param subscriptionName the name to use for the subscription to the consumed
	 * topic(s) or {@code null} to use the default configured subscription name. Beware
	 * that specifying {@code null} when no default subscription name is configured will
	 * result in an exception
	 * @param customizer an optional customizer to apply to the consumer builder. Note
	 * that the customizer is applied last and has the potential for overriding any
	 * specified parameters or default properties.
	 * @return the consumer
	 * @throws PulsarException if any {@link PulsarClientException} occurs communicating
	 * with Pulsar
	 */
	Consumer<T> createConsumer(Schema<T> schema, @Nullable Collection<String> topics, @Nullable String subscriptionName,
			ConsumerBuilderCustomizer<T> customizer);

	/**
	 * Create a consumer.
	 * @param schema the schema of the messages to be sent
	 * @param topics the topics the consumer will subscribe to, replacing the default
	 * topics, or {@code null} to use the default topics. Beware that using
	 * {@link ConsumerBuilder#topic} or {@link ConsumerBuilder#topics} will add to the
	 * default topics, not override them. Also beware that specifying {@code null} when no
	 * default topic is configured will result in an exception.
	 * @param subscriptionName the name to use for the subscription to the consumed
	 * topic(s) or {@code null} to use the default configured subscription name. Beware
	 * that specifying {@code null} when no default subscription name is configured will
	 * result in an exception
	 * @param metadataProperties the metadata properties to attach to the consumer,
	 * replacing the default metadata properties, or {@code null} to use the default
	 * metadata properties. Beware that using {@link ConsumerBuilder#property} or
	 * {@link ConsumerBuilder#properties} will add to the default metadata properties, not
	 * replace them.
	 * @param customizers the optional list of customizers to apply to the consumer
	 * builder. Note that the customizers are applied last and have the potential for
	 * overriding any specified parameters or default properties.
	 * @return the consumer
	 * @throws PulsarException if any {@link PulsarClientException} occurs communicating
	 * with Pulsar
	 */
	Consumer<T> createConsumer(Schema<T> schema, @Nullable Collection<String> topics, @Nullable String subscriptionName,
			@Nullable Map<String, String> metadataProperties, @Nullable List<ConsumerBuilderCustomizer<T>> customizers);

}
