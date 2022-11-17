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

import java.util.Collections;
import java.util.List;

import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.lang.Nullable;
import org.springframework.pulsar.listener.reactive.ReactivePulsarMessageListenerContainer;
import org.springframework.pulsar.support.MessageConverter;

/**
 * Adapter to avoid having to implement all methods.
 *
 * @param <T> Message payload type.
 * @author Christophe Bornet
 */
public class ReactivePulsarListenerEndpointAdapter<T> implements ReactivePulsarListenerEndpoint<T> {

	@Override
	public String getId() {
		return null;
	}

	@Override
	public String getSubscriptionName() {
		return null;
	}

	@Override
	public SubscriptionType getSubscriptionType() {
		return SubscriptionType.Exclusive;
	}

	@Override
	public List<String> getTopics() {
		return Collections.emptyList();
	}

	@Override
	public String getTopicPattern() {
		return null;
	}

	@Override
	public Boolean getAutoStartup() {
		return null;
	}

	@Override
	public void setupListenerContainer(ReactivePulsarMessageListenerContainer<T> listenerContainer,
			MessageConverter messageConverter) {
	}

	@Override
	public SchemaType getSchemaType() {
		return null;
	}

	@Nullable
	@Override
	public Integer getConcurrency() {
		return null;
	}

	@Override
	public boolean isFluxListener() {
		return false;
	}

	@Override
	public Boolean getUseKeyOrderedProcessing() {
		return null;
	}

}
