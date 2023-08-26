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
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;

import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

/**
 * Default implementation for {@link ReactivePulsarReaderFactory}.
 *
 * @param <T> underlying payload type for the reactive reader.
 * @author Christophe Bornet
 * @author Chris Bono
 */
public class DefaultReactivePulsarReaderFactory<T> implements ReactivePulsarReaderFactory<T> {

	private final ReactivePulsarClient reactivePulsarClient;

	@Nullable
	private final List<ReactiveMessageReaderBuilderCustomizer<T>> defaultConfigCustomizers;

	/**
	 * Construct an instance.
	 * @param reactivePulsarClient the reactive client
	 * @param defaultConfigCustomizers the optional list of customizers that defines the
	 * default configuration for each created reader.
	 */
	public DefaultReactivePulsarReaderFactory(ReactivePulsarClient reactivePulsarClient,
			List<ReactiveMessageReaderBuilderCustomizer<T>> defaultConfigCustomizers) {
		this.reactivePulsarClient = reactivePulsarClient;
		this.defaultConfigCustomizers = defaultConfigCustomizers;
	}

	@Override
	public ReactiveMessageReader<T> createReader(Schema<T> schema) {
		return createReader(schema, Collections.emptyList());
	}

	@Override
	public ReactiveMessageReader<T> createReader(Schema<T> schema,
			List<ReactiveMessageReaderBuilderCustomizer<T>> customizers) {

		ReactiveMessageReaderBuilder<T> readerBuilder = this.reactivePulsarClient.messageReader(schema);

		// Apply the default customizers
		if (!CollectionUtils.isEmpty(this.defaultConfigCustomizers)) {
			this.defaultConfigCustomizers.forEach((customizer -> customizer.customize(readerBuilder)));
		}

		// Apply the user specified customizers
		if (!CollectionUtils.isEmpty(customizers)) {
			customizers.forEach((c) -> c.customize(readerBuilder));
		}

		return readerBuilder.build();
	}

}
