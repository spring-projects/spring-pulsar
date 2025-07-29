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

package org.springframework.pulsar.reactive.listener.adapter;

import java.lang.reflect.Method;

import org.apache.pulsar.client.api.Message;
import org.reactivestreams.Publisher;

import org.springframework.pulsar.listener.adapter.HandlerAdapter;
import org.springframework.pulsar.reactive.listener.ReactivePulsarMessageHandler;
import org.springframework.pulsar.reactive.listener.ReactivePulsarOneByOneMessageHandler;

import reactor.core.publisher.Mono;

/**
 * A {@link ReactivePulsarMessageHandler MessageListener} adapter that invokes a
 * configurable {@link HandlerAdapter}; used when the factory is configured for the
 * listener to receive individual messages.
 *
 * @param <V> payload type.
 * @author Christophe Bornet
 * @author Soby Chacko
 */
@SuppressWarnings("NullAway")
public class PulsarReactiveOneByOneMessagingMessageListenerAdapter<V>
		extends PulsarReactiveMessagingMessageListenerAdapter<V> implements ReactivePulsarOneByOneMessageHandler<V> {

	public PulsarReactiveOneByOneMessagingMessageListenerAdapter(Object bean, Method method) {
		super(bean, method);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Publisher<Void> received(Message<V> record) {
		org.springframework.messaging.Message<?> message = null;
		Object theRecord = record;
		if (isHeaderFound() || isSpringMessage()) {
			message = toMessagingMessage(record, null);
		}
		else if (isSimpleExtraction()) {
			theRecord = record.getValue();
		}

		if (logger.isDebugEnabled()) {
			this.logger.debug("Processing [" + message + "]");
		}
		try {
			return (Mono<Void>) invokeHandler(message, theRecord, null, null);
		}
		catch (Exception e) {
			return Mono.error(e);
		}
	}

}
