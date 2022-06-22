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

package org.springframework.pulsar.support.converter;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;

import org.apache.pulsar.client.api.Consumer;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;

/**
 * @author Soby Chacko
 */
public class PulsarMessagingMessageConverter<V> implements PulsarRecordMessageConverter<V> {

	private SmartMessageConverter messagingConverter;

	@Override
	public Message<?> toMessage(org.apache.pulsar.client.api.Message<V> record, Consumer<V> consumer, Type type) {

		Message<?> message = MessageBuilder.createMessage(extractAndConvertValue(record, type), new MessageHeaders(Collections.emptyMap()));
		if (this.messagingConverter != null) {
			Class<?> clazz = type instanceof Class ? (Class<?>) type : type instanceof ParameterizedType
					? (Class<?>) ((ParameterizedType) type).getRawType() : Object.class;
			Object payload = this.messagingConverter.fromMessage(message, clazz, type);
			if (payload != null) {
				message = new GenericMessage<>(payload, message.getHeaders());
			}
		}
		return message;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public V fromMessage(Message<?> messageArg, String defaultTopic) {
		Message<?> message = messageArg;
		if (this.messagingConverter != null) {
			Message<?> converted = this.messagingConverter.toMessage(message.getPayload(), message.getHeaders());
			if (converted != null) {
				message = converted;
			}
		}
		return null; //TODO
	}



	protected org.springframework.messaging.converter.MessageConverter getMessagingConverter() {
		return this.messagingConverter;
	}

	public void setMessagingConverter(SmartMessageConverter messagingConverter) {
		this.messagingConverter = messagingConverter;
	}

	protected Object extractAndConvertValue(org.apache.pulsar.client.api.Message<V> record, Type type) {
		return record.getValue();
	}
}
