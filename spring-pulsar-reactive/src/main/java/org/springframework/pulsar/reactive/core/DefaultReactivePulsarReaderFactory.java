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

import java.util.Collections;
import java.util.List;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.api.ReactiveMessageReader;
import org.apache.pulsar.reactive.client.api.ReactiveMessageReaderBuilder;
import org.apache.pulsar.reactive.client.api.ReactiveMessageReaderSpec;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;

import org.springframework.util.CollectionUtils;

/**
 * Default implementation for {@link ReactivePulsarReaderFactory}.
 *
 * @param <T> underlying payload type for the reactive reader.
 * @author Christophe Bornet
 */
public class DefaultReactivePulsarReaderFactory<T> implements ReactivePulsarReaderFactory<T> {

	private final ReactiveMessageReaderSpec readerSpec;

	private final ReactivePulsarClient reactivePulsarClient;

	public DefaultReactivePulsarReaderFactory(ReactivePulsarClient reactivePulsarClient,
			ReactiveMessageReaderSpec readerSpec) {
		this.reactivePulsarClient = reactivePulsarClient;
		this.readerSpec = readerSpec;
	}

	@Override
	public ReactiveMessageReader<T> createReader(Schema<T> schema) {
		return createReader(schema, Collections.emptyList());
	}

	@Override
	public ReactiveMessageReader<T> createReader(Schema<T> schema,
			List<ReactiveMessageReaderBuilderCustomizer<T>> customizers) {

		ReactiveMessageReaderBuilder<T> reader = this.reactivePulsarClient.messageReader(schema)
			.applySpec(this.readerSpec);
		if (!CollectionUtils.isEmpty(customizers)) {
			customizers.forEach((c) -> c.customize(reader));
		}
		return reader.build();
	}

}
