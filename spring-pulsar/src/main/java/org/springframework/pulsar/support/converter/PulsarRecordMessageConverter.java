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

package org.springframework.pulsar.support.converter;

import java.lang.reflect.Type;

import org.apache.pulsar.client.api.Consumer;

import org.springframework.messaging.Message;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.pulsar.support.PulsarNull;
import org.springframework.pulsar.support.header.PulsarHeaderMapper;

/**
 *
 * A Messaging {@link org.springframework.pulsar.support.MessageConverter} implementation
 * for a message listener that receives individual messages.
 *
 * @param <V> message type
 * @author Soby Chacko
 * @author Chris Bono
 */
public class PulsarRecordMessageConverter<V> implements PulsarMessageConverter<V> {

	private final PulsarHeaderMapper headerMapper;

	private SmartMessageConverter messagingConverter;

	public PulsarRecordMessageConverter(PulsarHeaderMapper headerMapper) {
		this.headerMapper = headerMapper;
	}

	@Override
	public Message<?> toMessage(org.apache.pulsar.client.api.Message<V> record, Consumer<V> consumer, Type type) {
		return MessageBuilder.createMessage(extractAndConvertValue(record), this.headerMapper.toSpringHeaders(record));
	}

	protected org.springframework.messaging.converter.MessageConverter getMessagingConverter() {
		return this.messagingConverter;
	}

	public void setMessagingConverter(SmartMessageConverter messagingConverter) {
		this.messagingConverter = messagingConverter;
	}

	protected Object extractAndConvertValue(org.apache.pulsar.client.api.Message<V> record) {
		return record.getValue() != null ? record.getValue() : PulsarNull.INSTANCE;
	}

}
