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

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;

/**
 * @author Soby Chacko
 */
public class PulsarRecordMessagingMessageListenerAdapter<V> extends PulsarMessagingMessageListenerAdapter<V>
		implements MessageListener<V> {

	public PulsarRecordMessagingMessageListenerAdapter(Object bean, Method method) {
		super(bean, method);
	}

	@Override
	public void received(Consumer<V> consumer, Message<V> record) {
		org.springframework.messaging.Message<?> message = null;
		if (isConversionNeeded()) {
			message = toMessagingMessage(record, consumer);
		}
		else {
			//message = NULL_MESSAGE;
		}
		if (logger.isDebugEnabled()) {
			this.logger.debug("Processing [" + message + "]");
		}
		try {
			Object result = invokeHandler(record, message, consumer);
			if (result != null) {
				//handleResult(result, record, message);
			}
		}
		catch (Exception e) { // NOSONAR ex flow control
			throw e;
		}
	}

}
