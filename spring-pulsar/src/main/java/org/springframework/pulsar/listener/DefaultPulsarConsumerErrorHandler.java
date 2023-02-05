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

package org.springframework.pulsar.listener;

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
 * Note: This implementation uses a ThreadLocal to manage the current message in error and
 * it's associated BackOffExecution.
 *
 * @param <T> payload type managed by the Pulsar consumer
 * @author Soby Chacko
 */
public class DefaultPulsarConsumerErrorHandler<T> implements PulsarConsumerErrorHandler<T> {

	private final PulsarMessageRecovererFactory<T> pulsarMessageRecovererFactory;

	private final BackOff backOff;

	private final ThreadLocal<Pair> backOffExecutionThreadLocal = new ThreadLocal<>();

	public DefaultPulsarConsumerErrorHandler(PulsarMessageRecovererFactory<T> pulsarMessageRecovererFactory,
			BackOff backOff) {
		this.pulsarMessageRecovererFactory = pulsarMessageRecovererFactory;
		this.backOff = backOff;
	}

	@Override
	public boolean shouldRetryMessage(Exception exception, Message<T> message) {
		Pair pair = this.backOffExecutionThreadLocal.get();
		long nextBackOff;
		BackOffExecution backOffExecution;
		if (pair != null && pair.message.equals(message)) {
			backOffExecution = pair.backOffExecution;
		}
		else {
			backOffExecution = this.backOff.start();
			this.backOffExecutionThreadLocal.set(new Pair(message, backOffExecution));
		}
		nextBackOff = backOffExecution.nextBackOff();
		onNextBackoff(nextBackOff);
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
	public void recoverMessage(Consumer<T> consumer, Message<T> message, Exception exception) {
		this.pulsarMessageRecovererFactory.recovererForConsumer(consumer).recoverMessage(message, exception);
	}

	@SuppressWarnings("unchecked")
	public Message<T> currentMessage() {
		// there is only one message tracked at any time.
		Pair pair = this.backOffExecutionThreadLocal.get();
		if (pair == null) {
			return null;
		}
		return (Message<T>) pair.message();
	}

	public void clearMessage() {
		this.backOffExecutionThreadLocal.remove();
	}

	private record Pair(Message<?> message, BackOffExecution backOffExecution) {
	};

}
