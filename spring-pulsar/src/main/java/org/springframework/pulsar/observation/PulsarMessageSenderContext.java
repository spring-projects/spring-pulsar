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

package org.springframework.pulsar.observation;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.springframework.pulsar.observation.PulsarMessageSenderContext.MessageHolder;

import io.micrometer.observation.transport.SenderContext;

/**
 * {@link SenderContext} for Pulsar messages.
 *
 * @author Chris Bono
 */
public final class PulsarMessageSenderContext extends SenderContext<MessageHolder> {

	private final String beanName;

	private final String destination;

	private PulsarMessageSenderContext(MessageHolder messageHolder, String topic, String beanName) {
		super((carrier, key, value) -> messageHolder.property(key, value));
		setCarrier(messageHolder);
		this.beanName = beanName;
		this.destination = topic;
	}

	public static PulsarMessageSenderContext newContext(String topic, String beanName) {
		MessageHolder messageHolder = new MessageHolder();
		PulsarMessageSenderContext senderContext = new PulsarMessageSenderContext(messageHolder, topic, beanName);
		return senderContext;
	}

	public Map<String, String> properties() {
		return Objects.requireNonNull(getCarrier(), "Carrier should never be null").properties();
	}

	/**
	 * The name of the bean sending the message (typically a {@code PulsarTemplate}).
	 * @return the name of the bean sending the message
	 */
	public String getBeanName() {
		return this.beanName;
	}

	/**
	 * The destination topic for the message.
	 * @return the topic the message is being sent to
	 */
	public String getDestination() {
		return this.destination;
	}

	/**
	 * Acts as a carrier for a Pulsar message and records the propagated properties for
	 * later access by the Pulsar message builder.
	 */
	public static final class MessageHolder {

		private final Map<String, String> properties = new HashMap<>();

		private MessageHolder() {
		}

		public void property(String key, String value) {
			this.properties.put(key, value);
		}

		public Map<String, String> properties() {
			return this.properties;
		}

	}

}
