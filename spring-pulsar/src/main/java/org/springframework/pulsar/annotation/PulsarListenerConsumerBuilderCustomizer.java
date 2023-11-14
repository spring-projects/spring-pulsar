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

package org.springframework.pulsar.annotation;

import org.apache.pulsar.client.api.ConsumerBuilder;

import org.springframework.pulsar.core.ConsumerBuilderCustomizer;

/**
 * Callback interface that can be implemented by a bean to customize the
 * {@link ConsumerBuilder} that is used to create the underlying Pulsar consumer used by a
 * {@link PulsarListener} to receive messages.
 *
 * <p>
 * Unlike the {@link ConsumerBuilderCustomizer} which is applied to all created consumer
 * builders, this customizer is only applied to the individual consumer builder(s) of the
 * {@code @PulsarListener(s)} it is associated with.
 *
 * @param <T> The message payload type
 * @author Chris Bono
 */
@FunctionalInterface
public interface PulsarListenerConsumerBuilderCustomizer<T> {

	/**
	 * Customize the {@link ConsumerBuilder}.
	 * @param consumerBuilder the builder to customize
	 */
	void customize(ConsumerBuilder<T> consumerBuilder);

}
