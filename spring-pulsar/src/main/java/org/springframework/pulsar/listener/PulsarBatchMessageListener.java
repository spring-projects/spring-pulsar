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

/**
 * @author Soby Chacko
 */
package org.springframework.pulsar.listener;

import java.util.List;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;

/**
 *
 * Batch message listener.
 *
 * @param <T> payload type.
 * @author Soby Chacko
 */
public interface PulsarBatchMessageListener<T> extends PulsarRecordMessageListener<T> {

	default void received(Consumer<T> consumer, Message<T> msg) {
		throw new UnsupportedOperationException();
	}

	default void received(Consumer<T> consumer, List<Message<T>> msg, Acknowledgement acknowledgement) {
		throw new UnsupportedOperationException();
	}

	void received(Consumer<T> consumer, List<Message<T>> msg);

}
