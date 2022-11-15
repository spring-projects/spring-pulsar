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

package org.springframework.pulsar.config.reactive;

import java.util.List;

import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.lang.Nullable;
import org.springframework.pulsar.config.GenericPulsarListenerEndpoint;
import org.springframework.pulsar.listener.reactive.ReactivePulsarMessageListenerContainer;
import org.springframework.pulsar.support.MessageConverter;

/**
 * Model for a Pulsar reactive listener endpoint. Can be used against a
 * {@link org.springframework.pulsar.annotation.PulsarListenerConfigurer} to register
 * endpoints programmatically.
 *
 * @param <T> Message payload type.
 * @author Christophe Bornet
 */
public interface ReactivePulsarListenerEndpoint<T> extends GenericPulsarListenerEndpoint {

	@Nullable
	SubscriptionType getSubscriptionType();

	List<String> getTopics();

	String getTopicPattern();

	@Nullable
	Boolean getAutoStartup();

	void setupListenerContainer(ReactivePulsarMessageListenerContainer<T> listenerContainer,
			@Nullable MessageConverter messageConverter);

	SchemaType getSchemaType();

	@Nullable
	Integer getConcurrency();

}
