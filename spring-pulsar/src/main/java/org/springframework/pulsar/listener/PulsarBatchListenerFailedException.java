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

package org.springframework.pulsar.listener;

import org.apache.pulsar.client.api.Message;

import org.springframework.pulsar.PulsarException;

/**
 * Batch message listeners should throw this exception in the event of an error.
 *
 * @author Soby Chacko
 */
public class PulsarBatchListenerFailedException extends PulsarException {

	private final Object messageInError;

	public PulsarBatchListenerFailedException(String msg, Message<?> message) {
		super(msg);
		this.messageInError = message;
	}

	public PulsarBatchListenerFailedException(String msg, Throwable cause, Message<?> message) {
		super(msg, cause);
		this.messageInError = message;
	}

	public Object getMessageInError() {
		return this.messageInError;
	}

}
