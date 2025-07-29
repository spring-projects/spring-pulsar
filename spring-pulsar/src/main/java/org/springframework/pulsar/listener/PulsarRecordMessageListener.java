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

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;

/**
 * Base record MessageListener that simply extends from {@link MessageListener}. This
 * extension is needed as a base class to deal with acknowledgments in the framework.
 *
 * @param <T> message payload type
 * @author Soby Chacko
 */
public interface PulsarRecordMessageListener<T> extends MessageListener<T> {

	default void received(Consumer<T> consumer, Message<T> msg, Acknowledgement acknowledgement) {
		throw new UnsupportedOperationException("Not supported");
	}

}
