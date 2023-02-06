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

package org.springframework.pulsar.reactive.listener;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.reactive.client.api.MessageResult;
import org.apache.pulsar.reactive.client.api.ReactiveMessagePipelineBuilder;
import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;

/**
 * Message handler class with a {@link #received} method for use in
 * {@link ReactiveMessagePipelineBuilder#streamingMessageHandler}.
 *
 * @param <T> message payload type
 * @author Christophe Bornet
 */
public non-sealed interface ReactivePulsarStreamingHandler<T> extends ReactivePulsarMessageHandler {

	/**
	 * Callback passed to {@link ReactiveMessagePipelineBuilder#streamingMessageHandler}
	 * that will be applied to the flux of received message.
	 * @param messages the messages received
	 * @return a completed {@link Publisher} when the callback is done.
	 */
	Publisher<MessageResult<Void>> received(Flux<Message<T>> messages);

}
