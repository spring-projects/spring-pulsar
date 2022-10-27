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

package org.springframework.pulsar.core.reactive;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.api.ReactiveMessageReader;
import org.apache.pulsar.reactive.client.api.ReactiveMessageReaderBuilder;
import org.apache.pulsar.reactive.client.api.ReactiveMessageReaderSpec;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;

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
	public ReactiveMessageReader<T> createReader(Schema<T> schema, ReactiveMessageReaderSpec spec) {

		ReactiveMessageReaderBuilder<T> readerBuilder = this.reactivePulsarClient.messageReader(schema)
				.applySpec(this.readerSpec);
		if (spec != null) {
			readerBuilder.applySpec(spec);
		}
		return readerBuilder.build();
	}

}
