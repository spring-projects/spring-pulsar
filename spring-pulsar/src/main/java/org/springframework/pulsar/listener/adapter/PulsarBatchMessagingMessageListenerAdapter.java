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

package org.springframework.pulsar.listener.adapter;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Messages;

import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.pulsar.support.converter.PulsarBatchMessageConverter;
import org.springframework.pulsar.listener.PulsarBatchMessageListener;
import org.springframework.pulsar.support.converter.PulsarBatchMessagingMessageConverter;
import org.springframework.pulsar.support.converter.PulsarRecordMessageConverter;
import org.springframework.util.Assert;

/**
 * @author Soby Chacko
 */
public class PulsarBatchMessagingMessageListenerAdapter <V> extends PulsarMessagingMessageListenerAdapter<V>
		implements PulsarBatchMessageListener<V> {

	private PulsarBatchMessageConverter<V> batchMessageConverter = new PulsarBatchMessagingMessageConverter<V>();

	public PulsarBatchMessagingMessageListenerAdapter(Object bean, Method method) {
		super(bean, method);
	}

	public void setBatchMessageConverter(PulsarBatchMessageConverter<V> messageConverter) {
		Assert.notNull(messageConverter, "'messageConverter' cannot be null");
		this.batchMessageConverter = messageConverter;
		PulsarRecordMessageConverter<V> recordMessageConverter = messageConverter.getRecordMessageConverter();
		if (recordMessageConverter != null) {
			setMessageConverter(recordMessageConverter);
		}
	}

	protected final PulsarBatchMessageConverter<V> getBatchMessageConverter() {
		return this.batchMessageConverter;
	}

	public void received(Consumer<V> consumer, Messages<V> msg) {
		Message<?> message;
		if (!isConsumerRecordList()) {
			if (isMessageList()) {
				List<Message<?>> messages = new ArrayList<>(msg.size());
				for (org.apache.pulsar.client.api.Message<V> record : msg) {
					messages.add(toMessagingMessage(record, consumer));
				}
				message = MessageBuilder.withPayload(messages).build();
			}
			else {
				message = toMessagingMessage(msg, consumer);
			}
		}
		else {
			message = null; // optimization since we won't need any conversion to invoke
		}
		logger.debug(() -> "Processing [" + message + "]");
		invoke(msg, consumer, message);
	}

	protected void invoke(Object records, Consumer<V> consumer,
						  final Message<?> messageArg) {

		Message<?> message = messageArg;
		try {
			Object result = invokeHandler(records, message, consumer);
//			if (result != null) {
//				handleResult(result, records, message);
//			}
		}
		catch (Exception e) {
			throw e;
		}
	}


	protected Message<?> toMessagingMessage(Messages<V> msg, Consumer<V> consumer) {

		return getBatchMessageConverter().toMessage(msg, consumer, getType());
	}

}
