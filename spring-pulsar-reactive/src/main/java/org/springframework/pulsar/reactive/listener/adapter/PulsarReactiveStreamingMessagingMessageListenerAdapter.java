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

package org.springframework.pulsar.reactive.listener.adapter;

import java.lang.reflect.Method;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.reactive.client.api.MessageResult;

import org.springframework.pulsar.listener.adapter.HandlerAdapter;
import org.springframework.pulsar.reactive.listener.ReactivePulsarMessageHandler;
import org.springframework.pulsar.reactive.listener.ReactivePulsarStreamingHandler;

import reactor.core.publisher.Flux;

/**
 * A {@link ReactivePulsarMessageHandler MessageListener} adapter that invokes a
 * configurable {@link HandlerAdapter}; used when the factory is configured for the
 * listener to receive a flux of messages.
 *
 * @param <V> payload type.
 * @author Christophe Bornet
 * @author Soby Chacko
 */
public class PulsarReactiveStreamingMessagingMessageListenerAdapter<V>
		extends PulsarReactiveMessagingMessageListenerAdapter<V> implements ReactivePulsarStreamingHandler<V> {

	public PulsarReactiveStreamingMessagingMessageListenerAdapter(Object bean, Method method) {
		super(bean, method);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Flux<MessageResult<Void>> received(Flux<Message<V>> records) {
		Flux<?> theRecords = records;
		if (isSpringMessageFlux()) {
			theRecords = records.map(record -> toMessagingMessage(record, null));
		}
		try {
			return (Flux<MessageResult<Void>>) invokeHandler(null, theRecords, null, null);
		}
		catch (Exception e) {
			return Flux.error(e);
		}
	}

}
