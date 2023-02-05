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
import org.apache.pulsar.reactive.client.api.ImmutableReactiveMessageConsumerSpec;
import org.apache.pulsar.reactive.client.api.MutableReactiveMessageConsumerSpec;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumer;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumerBuilder;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumerSpec;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;

import org.springframework.util.CollectionUtils;

/**
 * Default implementation for {@link ReactivePulsarConsumerFactory}.
 *
 * @param <T> underlying payload type for the reactive consumer.
 * @author Christophe Bornet
 */
public class DefaultReactivePulsarConsumerFactory<T> implements ReactivePulsarConsumerFactory<T> {

	private final ReactiveMessageConsumerSpec consumerSpec;

	private final ReactivePulsarClient reactivePulsarClient;

	public DefaultReactivePulsarConsumerFactory(ReactivePulsarClient reactivePulsarClient,
			ReactiveMessageConsumerSpec consumerSpec) {
		this.consumerSpec = new ImmutableReactiveMessageConsumerSpec(
				consumerSpec != null ? consumerSpec : new MutableReactiveMessageConsumerSpec());
		this.reactivePulsarClient = reactivePulsarClient;
	}

	@Override
	public ReactiveMessageConsumer<T> createConsumer(Schema<T> schema) {
		return createConsumer(schema, Collections.emptyList());
	}

	@Override
	public ReactiveMessageConsumer<T> createConsumer(Schema<T> schema,
			List<ReactiveMessageConsumerBuilderCustomizer<T>> customizers) {

		ReactiveMessageConsumerBuilder<T> consumer = this.reactivePulsarClient.messageConsumer(schema);

		consumer.applySpec(this.consumerSpec);
		if (!CollectionUtils.isEmpty(customizers)) {
			customizers.forEach((c) -> c.customize(consumer));
		}

		return consumer.build();
	}

}
