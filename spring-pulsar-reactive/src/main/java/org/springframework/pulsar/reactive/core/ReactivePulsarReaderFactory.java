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

package org.springframework.pulsar.reactive.core;

import java.util.List;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.api.ReactiveMessageReader;

/**
 * The strategy to create a {@link ReactiveMessageReader} instance(s).
 *
 * @param <T> reactive message reader payload type
 * @author Christophe Bornet
 */
public interface ReactivePulsarReaderFactory<T> {

	/**
	 * Create a reactive message reader.
	 * @param schema the schema of the messages to be read
	 * @return the reactive message reader
	 */
	ReactiveMessageReader<T> createReader(Schema<T> schema);

	/**
	 * Create a reactive message reader.
	 * @param schema the schema of the messages to be read
	 * @param customizers the optional list of readers to apply to the reactive message
	 * reader builder
	 * @return the reactive message reader
	 */
	ReactiveMessageReader<T> createReader(Schema<T> schema,
			List<ReactiveMessageReaderBuilderCustomizer<T>> customizers);

}
