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

import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumerBuilder;

/**
 * The interface to customize a {@link ReactiveMessageConsumerBuilder}.
 *
 * @param <T> The message payload type
 * @author Christophe Bornet
 */
@FunctionalInterface
public interface ReactiveMessageConsumerBuilderCustomizer<T> {

	/**
	 * Customizes a {@link ReactiveMessageConsumerBuilder}.
	 * @param reactiveMessageConsumerBuilder the builder to customize
	 */
	void customize(ReactiveMessageConsumerBuilder<T> reactiveMessageConsumerBuilder);

}
