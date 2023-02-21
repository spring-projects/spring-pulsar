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
import java.lang.reflect.Type;
import java.util.List;

import org.apache.pulsar.client.api.Messages;

import org.springframework.pulsar.listener.adapter.AbstractPulsarMessageToSpringMessageAdapter;
import org.springframework.pulsar.reactive.listener.ReactivePulsarMessageHandler;

import reactor.core.publisher.Flux;

/**
 * An abstract base for {@link ReactivePulsarMessageHandler MessageListener} adapters.
 *
 * @param <V> payload type.
 * @author Chris Bono
 */
public abstract class PulsarReactiveMessagingMessageListenerAdapter<V>
		extends AbstractPulsarMessageToSpringMessageAdapter<V> {

	public PulsarReactiveMessagingMessageListenerAdapter(Object bean, Method method) {
		super(bean, method);
	}

	/**
	 * Determines if a type is one that holds multiple messages.
	 * @param type the type to check
	 * @return true if the type is a {@link List}, {@link Messages} or {@link Flux}, false
	 * otherwise
	 */
	protected boolean isMultipleMessageType(Type type) {
		return super.isMultipleMessageType(type) || parameterIsType(type, Flux.class);
	}

	/**
	 * Determine if the type is a reactive Flux.
	 * @param type type to check
	 * @return true if the type is a reactive Flux, false otherwise
	 */
	@Override
	protected boolean isFlux(Type type) {
		return Flux.class.equals(type);
	}

}
