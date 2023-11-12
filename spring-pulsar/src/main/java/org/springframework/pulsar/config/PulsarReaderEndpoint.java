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

package org.springframework.pulsar.config;

import java.util.List;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.lang.Nullable;
import org.springframework.pulsar.reader.PulsarMessageReaderContainer;
import org.springframework.pulsar.support.MessageConverter;

/**
 * Model for a Pulsar Reader endpoint. Can be used against a
 * {@link org.springframework.pulsar.annotation.PulsarReaderConfigurer} to register
 * endpoints programmatically.
 *
 * @param <C> reader listener container type.
 * @author Soby Chacko
 */
public interface PulsarReaderEndpoint<C extends PulsarMessageReaderContainer> {

	/**
	 * Return the id of this endpoint.
	 * @return the id of this endpoint. The id can be further qualified when the endpoint
	 * is resolved against its actual listener container.
	 * @see ListenerContainerFactory#createListenerContainer
	 */
	@Nullable
	String getId();

	/**
	 * Return the topics for this endpoint's container.
	 * @return the topics.
	 */
	List<String> getTopics();

	/**
	 * Return the schema type for this endpoint's container.
	 * @return the schema type.
	 */
	SchemaType getSchemaType();

	/**
	 * Setup the specified message listener container with the model defined by this
	 * endpoint.
	 * <p>
	 * This endpoint must provide the requested missing option(s) of the specified
	 * container to make it usable. Usually, this is about setting the {@code queues} and
	 * the {@code messageListener} to use but an implementation may override any default
	 * setting that was already set.
	 * @param listenerContainer the listener container to configure
	 * @param messageConverter message converter used
	 */
	void setupListenerContainer(C listenerContainer, @Nullable MessageConverter messageConverter);

	@Nullable
	Boolean getAutoStartup();

	MessageId getStartMessageId();

}
