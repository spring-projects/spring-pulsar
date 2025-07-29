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

package org.springframework.pulsar.reactive.listener;

import org.springframework.pulsar.listener.MessageListenerContainer;
import org.springframework.pulsar.reactive.core.ReactiveMessageConsumerBuilderCustomizer;

/**
 * Internal abstraction used by the framework representing a reactive message listener
 * container. Not meant to be implemented externally.
 *
 * @param <T> message type.
 * @author Christophe Bornet
 */
public sealed interface ReactivePulsarMessageListenerContainer<T>
		extends MessageListenerContainer permits DefaultReactivePulsarMessageListenerContainer {

	void setupMessageHandler(ReactivePulsarMessageHandler messageListener);

	default ReactivePulsarContainerProperties<T> getContainerProperties() {
		throw new UnsupportedOperationException("This container doesn't support retrieving its properties");
	}

	void setConsumerCustomizer(ReactiveMessageConsumerBuilderCustomizer<T> consumerCustomizer);

}
