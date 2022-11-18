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

package org.springframework.pulsar.reactive.core;

import java.util.List;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumer;

/**
 * Pulsar reactive consumer factory interface.
 *
 * @param <T> payload type for the consumer.
 * @author Christophe Bornet
 */
public interface ReactivePulsarConsumerFactory<T> {

	/**
	 * Create a reactive message consumer.
	 * @param schema the schema of the messages to be consumed
	 * @return the reactive message consumer
	 */
	ReactiveMessageConsumer<T> createConsumer(Schema<T> schema);

	/**
	 * Create a reactive message consumer.
	 * @param schema the schema of the messages to be consumed
	 * @param customizers the optional list of customizers to apply to the reactive
	 * message consumer builder
	 * @return the reactive message consumer
	 */
	ReactiveMessageConsumer<T> createConsumer(Schema<T> schema,
			List<ReactiveMessageConsumerBuilderCustomizer<T>> customizers);

}
