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

package org.springframework.pulsar.support.converter;

import java.lang.reflect.Type;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Reader;

import org.springframework.messaging.Message;
import org.springframework.pulsar.support.MessageConverter;

/**
 * Pulsar specific record converter strategy.
 *
 * @param <T> message type
 * @author Soby Chacko
 */
public interface PulsarMessageConverter<T> extends MessageConverter {

	Message<?> toMessage(org.apache.pulsar.client.api.Message<T> record, Consumer<T> consumer, Type payloadType);

	Message<?> toMessageFromReader(org.apache.pulsar.client.api.Message<T> record, Reader<T> reader, Type payloadType);

}
