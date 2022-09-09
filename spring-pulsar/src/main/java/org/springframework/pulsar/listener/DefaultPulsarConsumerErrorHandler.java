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

package org.springframework.pulsar.listener;

import java.util.Iterator;
import java.util.Map;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;

import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;

/**
 * Default implementation for {@link PulsarConsumerErrorHandler}.
 * <p>
 * This implementation is capable for handling errors based on the interface contract.
 * After handling the errors, if necessary, this implementation is capable of recovering
 * the record(s) using a {@link PulsarMessageRecoverer}
 *
 * @param <T> payload type managed by the Pulsar consumer
 * @author Soby Chacko
 */
public class DefaultPulsarConsumerErrorHandler<T> implements PulsarConsumerErrorHandler<T> {

	private final PulsarMessageRecoverer<T> pulsarConsumerRecordRecoverer;

	private final BackOff backOff;

	private final ThreadLocal<Map<Message<T>, BackOffExecution>> backOffExecutionThreadLocal = new ThreadLocal<>();

	public DefaultPulsarConsumerErrorHandler(PulsarMessageRecoverer<T> pulsarConsumerRecordRecoverer, BackOff backOff) {
		this.pulsarConsumerRecordRecoverer = pulsarConsumerRecordRecoverer;
		this.backOff = backOff;
	}

	@Override
	public boolean shouldRetryMessageInError(Exception thrownException, Message<T> message) {
		final Map<Message<T>, BackOffExecution> messageBackOffExecutionMap = this.backOffExecutionThreadLocal.get();
		long nextBackOff;
		if (messageBackOffExecutionMap != null && messageBackOffExecutionMap.containsKey(message)) {
			final BackOffExecution backOffExecution = messageBackOffExecutionMap.get(message);
			nextBackOff = backOffExecution.nextBackOff();
			onNextBackoff(nextBackOff);
		}
		else {
			final BackOffExecution backOffExecution = this.backOff.start();
			this.backOffExecutionThreadLocal.set(Map.of(message, backOffExecution));
			nextBackOff = backOffExecution.nextBackOff();
			onNextBackoff(nextBackOff);
		}
		return nextBackOff != BackOffExecution.STOP;
	}

	private void onNextBackoff(long nextBackOff) {
		if (nextBackOff > BackOffExecution.STOP) {
			try {
				Thread.sleep(nextBackOff);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	@Override
	public void recoverMessage(Consumer<T> consumer, Message<T> message, Exception thrownException) {
		this.pulsarConsumerRecordRecoverer.apply(consumer).accept(message, thrownException);
	}

	public Message<T> getTheCurrentPulsarMessageTracked() {
		// there is only one message tracked at any time.
		final Map<Message<T>, BackOffExecution> messageBackOffExecutionMap = this.backOffExecutionThreadLocal.get();
		if (messageBackOffExecutionMap == null) {
			return null;
		}
		final Iterator<Message<T>> iterator = messageBackOffExecutionMap.keySet().iterator();
		return iterator.hasNext() ? iterator.next() : null;
	}

	public void clearThreadState() {
		this.backOffExecutionThreadLocal.remove();
	}

}
