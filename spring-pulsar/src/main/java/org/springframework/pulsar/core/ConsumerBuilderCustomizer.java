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

package org.springframework.pulsar.core;

import org.apache.pulsar.client.api.ConsumerBuilder;

/**
 * Callback interface that can be implemented to customize the {@link ConsumerBuilder}
 * that is used by the {@link PulsarConsumerFactory} to create consumers.
 * <p>
 * When using Spring Boot autoconfiguration, any beans implementing this interface will be
 * used as default configuration by the {@link DefaultPulsarConsumerFactory} and as such
 * will apply to all created consumers.
 * <p>
 * The consumer factory also supports passing in a specific instance of this callback when
 * {@link PulsarConsumerFactory#createConsumer creating a consumer} and as such the passed
 * in customizer only applies to the single created consumer.
 *
 * @param <T> The message payload type
 * @author Christophe Bornet
 */
@FunctionalInterface
public interface ConsumerBuilderCustomizer<T> {

	/**
	 * Customize the {@link ConsumerBuilder}.
	 * @param consumerBuilder the builder to customize
	 */
	void customize(ConsumerBuilder<T> consumerBuilder);

}
