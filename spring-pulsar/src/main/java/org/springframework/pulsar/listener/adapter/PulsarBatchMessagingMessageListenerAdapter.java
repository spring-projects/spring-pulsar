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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Messages;

import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.pulsar.listener.Acknowledgement;
import org.springframework.pulsar.listener.PulsarBatchAcknowledgingMessageListener;
import org.springframework.pulsar.support.converter.PulsarBatchMessageConverter;
import org.springframework.pulsar.support.converter.PulsarBatchMessagingMessageConverter;
import org.springframework.pulsar.support.converter.PulsarRecordMessageConverter;
import org.springframework.util.Assert;

/**
 * A {@link org.apache.pulsar.client.api.MessageListener MessageListener} adapter that
 * invokes a configurable {@link HandlerAdapter}; used when the factory is configured for
 * the listener to receive batches of messages.
 *
 * @param <V> payload type.
 * @author Soby Chacko
 */
@SuppressWarnings("serial")
public class PulsarBatchMessagingMessageListenerAdapter<V> extends PulsarMessagingMessageListenerAdapter<V>
		implements PulsarBatchAcknowledgingMessageListener<V> {

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

	@Override
	public void received(Consumer<V> consumer, List<org.apache.pulsar.client.api.Message<V>> msg,
			@Nullable Acknowledgement acknowledgement) {
		Message<?> message = null;
		Object theRecord = null;

		if (isPulsarMessageList() && !isHeaderFound()) { // List<PulsarMessage>
			theRecord = msg; // the incoming list as is.
		}
		else if (isPulsarMessageList() && isHeaderFound()) { // List<PulsarMessage>,
																// @Header
			List<Message<?>> messages = toSpringMessages(consumer, msg);
			final Map<String, List<Object>> aggregatedHeaders = withAggregatedHeaders(messages);
			List<Object> list1 = new ArrayList<>(msg);
			message = MessageBuilder.withPayload(list1).copyHeaders(aggregatedHeaders).build();
		}
		else if (isMessageList() && !isHeaderFound()) { // List<SpringMessage>
			List<Message<?>> messages = toSpringMessages(consumer, msg);
			message = MessageBuilder.withPayload(messages).build();
		}
		else if (isMessageList() && isHeaderFound()) { // List<SpringMessage>, @Header
			List<Message<?>> messages = toSpringMessages(consumer, msg);
			final Map<String, List<Object>> aggregatedHeaders = withAggregatedHeaders(messages);
			message = MessageBuilder.withPayload(messages).copyHeaders(aggregatedHeaders).build();
		}
		else if (this.isSimpleExtraction()) { // List<Object>
			List<V> list = new ArrayList<>(msg.size());
			msg.stream().iterator().forEachRemaining(vMessage -> list.add(vMessage.getValue()));
			theRecord = list;
		}
		else if (isHeaderFound()) { // List<Object>, @Header
			List<Message<?>> messages = toSpringMessages(consumer, msg);
			final Map<String, List<Object>> aggregatedHeaders = withAggregatedHeaders(messages);
			List<V> list = new ArrayList<>(msg.size());
			msg.stream().iterator().forEachRemaining(vMessage -> list.add(vMessage.getValue()));
			message = MessageBuilder.withPayload(list).copyHeaders(aggregatedHeaders).build();
		}

		if (isConsumerRecords()) { // Messages<String>
			theRecord = new Messages<V>() {

				@Override
				public Iterator<org.apache.pulsar.client.api.Message<V>> iterator() {
					return msg.iterator();
				}

				@Override
				public int size() {
					return msg.size();
				}
			};
		}
		invoke(theRecord, consumer, message, acknowledgement);
	}

	private Map<String, List<Object>> withAggregatedHeaders(List<Message<?>> messages) {
		final Map<String, List<Object>> aggregatedHeaders = new HashMap<>();
		for (Message<?> message : messages) {
			message.getHeaders().forEach((s, o) -> {
				List<Object> objects = aggregatedHeaders.computeIfAbsent(s, k -> new ArrayList<>());
				objects.add(o);
			});
		}
		return aggregatedHeaders;
	}

	private List<Message<?>> toSpringMessages(Consumer<V> consumer, List<org.apache.pulsar.client.api.Message<V>> msg) {
		List<Message<?>> messages = new ArrayList<>(msg.size());
		msg.stream().iterator().forEachRemaining(record -> messages.add(toMessagingMessage(record, consumer)));
		return messages;
	}

	protected void invoke(Object records, Consumer<V> consumer, final Message<?> message,
			Acknowledgement acknowledgement) {
		try {
			invokeHandler(records, message, consumer, acknowledgement);
		}
		catch (Exception e) {
			throw e;
		}
	}

}
