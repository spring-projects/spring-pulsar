/*
 * Copyright 2022-2024 the original author or authors.
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

package org.springframework.pulsar.config;

import java.util.Collection;
import java.util.Collections;

import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.lang.Nullable;
import org.springframework.pulsar.listener.MessageListenerContainer;
import org.springframework.pulsar.support.MessageConverter;

/**
 * Model for a Pulsar listener endpoint. Can be used against a
 * {@link org.springframework.pulsar.annotation.PulsarListenerConfigurer} to register
 * endpoints programmatically.
 *
 * @param <C> Message listener container type.
 * @author Christophe Bornet
 * @author Vedran Pavic
 */
public interface ListenerEndpoint<C extends MessageListenerContainer> {

	/**
	 * Return the id of this endpoint.
	 * @return the id of this endpoint. The id can be further qualified when the endpoint
	 * is resolved against its actual listener container.
	 * @see ListenerContainerFactory#createRegisteredContainer
	 */
	@Nullable
	default String getId() {
		return null;
	}

	/**
	 * Return the subscription name for this endpoint's container.
	 * @return the subscription name.
	 */
	@Nullable
	default String getSubscriptionName() {
		return null;
	}

	/**
	 * Return the subscription type for this endpoint's container.
	 * @return the subscription type.
	 */
	@Nullable
	default SubscriptionType getSubscriptionType() {
		return SubscriptionType.Exclusive;
	}

	/**
	 * Return the topics for this endpoint's container.
	 * @return the topics.
	 */
	default Collection<String> getTopics() {
		return Collections.emptyList();
	}

	/**
	 * Return the topic pattern for this endpoint's container.
	 * @return the topic pattern.
	 */
	default String getTopicPattern() {
		return null;
	}

	/**
	 * Return the autoStartup for this endpoint's container.
	 * @return the autoStartup.
	 */
	@Nullable
	default Boolean getAutoStartup() {
		return null;
	}

	/**
	 * Return the schema type for this endpoint's container.
	 * @return the schema type.
	 */
	default SchemaType getSchemaType() {
		return null;
	}

	/**
	 * Return the concurrency for this endpoint's container.
	 * @return the concurrency.
	 */
	@Nullable
	default Integer getConcurrency() {
		return null;
	}

	/**
	 * Setup the specified message listener container with the model defined by this
	 * endpoint.
	 * <p>
	 * This endpoint must provide the requested missing option(s) of the specified
	 * container to make it usable. Usually, this is about setting the {@code queues} and
	 * the {@code messageListener} to use but an implementation may override any default
	 * setting that was already set.
	 * @param listenerContainer the listener container to configure
	 * @param messageConverter the message converter - can be null
	 */
	default void setupListenerContainer(C listenerContainer, @Nullable MessageConverter messageConverter) {
	}

}
