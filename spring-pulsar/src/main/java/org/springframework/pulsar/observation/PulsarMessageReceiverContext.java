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

package org.springframework.pulsar.observation;

import org.apache.pulsar.client.api.Message;

import io.micrometer.observation.transport.ReceiverContext;

/**
 * {@link ReceiverContext} for Pulsar messages.
 *
 * @author Chris Bono
 */
public class PulsarMessageReceiverContext extends ReceiverContext<Message<?>> {

	private final String listenerId;

	private final Message<?> message;

	public PulsarMessageReceiverContext(Message<?> message, String listenerId) {
		super((carrier, key) -> carrier.getProperty(key));
		setCarrier(message);
		this.message = message;
		this.listenerId = listenerId;
	}

	/**
	 * The identifier of the listener receiving the message (typically a
	 * {@code PulsarListener}).
	 * @return the identifier of the listener receiving the message
	 */
	public String getListenerId() {
		return this.listenerId;
	}

	/**
	 * The name of the topic the message came from.
	 * @return the name of the topic the message came from
	 */
	public String getSource() {
		return this.message.getTopicName();
	}

}
