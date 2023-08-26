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
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumer;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumerBuilder;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;

import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

/**
 * Default implementation for {@link ReactivePulsarConsumerFactory}.
 *
 * @param <T> underlying payload type for the reactive consumer.
 * @author Christophe Bornet
 * @author Chris Bono
 */
public class DefaultReactivePulsarConsumerFactory<T> implements ReactivePulsarConsumerFactory<T> {

	private final ReactivePulsarClient reactivePulsarClient;

	@Nullable
	private final List<ReactiveMessageConsumerBuilderCustomizer<T>> defaultConfigCustomizers;

	/**
	 * Construct an instance.
	 * @param reactivePulsarClient the reactive client
	 * @param defaultConfigCustomizers the optional list of customizers that defines the
	 * default configuration for each created consumer.
	 */
	public DefaultReactivePulsarConsumerFactory(ReactivePulsarClient reactivePulsarClient,
			List<ReactiveMessageConsumerBuilderCustomizer<T>> defaultConfigCustomizers) {
		this.reactivePulsarClient = reactivePulsarClient;
		this.defaultConfigCustomizers = defaultConfigCustomizers;
	}

	@Override
	public ReactiveMessageConsumer<T> createConsumer(Schema<T> schema) {
		return createConsumer(schema, Collections.emptyList());
	}

	@Override
	public ReactiveMessageConsumer<T> createConsumer(Schema<T> schema,
			List<ReactiveMessageConsumerBuilderCustomizer<T>> customizers) {

		ReactiveMessageConsumerBuilder<T> consumerBuilder = this.reactivePulsarClient.messageConsumer(schema);

		// Apply the default customizers
		if (!CollectionUtils.isEmpty(this.defaultConfigCustomizers)) {
			this.defaultConfigCustomizers.forEach((customizer -> customizer.customize(consumerBuilder)));
		}

		// Apply the user specified customizers
		if (!CollectionUtils.isEmpty(customizers)) {
			customizers.forEach((c) -> c.customize(consumerBuilder));
		}

		return consumerBuilder.build();
	}

}
