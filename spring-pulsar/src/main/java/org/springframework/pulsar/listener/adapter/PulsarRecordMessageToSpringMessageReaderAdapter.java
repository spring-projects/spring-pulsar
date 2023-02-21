/*
 * Copyright 2023 the original author or authors.
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

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderListener;

/**
 * A {@link MessageListener MessageListener} adapter that invokes a configurable
 * {@link HandlerAdapter}; used when the factory is configured for the listener to receive
 * individual messages.
 *
 * @param <V> payload type.
 * @author Soby Chacko
 */
public class PulsarRecordMessageToSpringMessageReaderAdapter<V> extends AbstractPulsarMessageToSpringMessageAdapter<V>
		implements ReaderListener<V> {

	public PulsarRecordMessageToSpringMessageReaderAdapter(Object bean, Method method) {
		super(bean, method);
	}

	@Override
	public void received(Reader<V> reader, Message<V> record) {
		org.springframework.messaging.Message<?> message = null;
		Object theRecord = record;
		if (isHeaderFound() || isSpringMessage()) {
			message = toMessagingMessageFromReader(record, reader);
		}
		else if (isSimpleExtraction()) {
			theRecord = record.getValue();
		}

		if (logger.isDebugEnabled()) {
			this.logger.debug("Processing [" + message + "]");
		}
		try {
			invokeHandler(message, theRecord, reader);
		}
		catch (Exception e) {
			throw e;
		}
	}

}
