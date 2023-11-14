/*
 * Copyright 2023 the original author or authors.
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

import org.apache.pulsar.client.api.ReaderBuilder;

import org.springframework.pulsar.core.ReaderBuilderCustomizer;

/**
 * Callback interface that can be implemented by a bean to customize the
 * {@link ReaderBuilder} that is used to create the underlying Pulsar reader used by a
 * {@link PulsarReader @PulsarReader} to receive messages.
 *
 * <p>
 * Unlike the {@link ReaderBuilderCustomizer} which is applied to all created reader
 * builders, this customizer is only applied to the individual reader builder(s) of the
 * {@code @PulsarReader(s)} it is associated with.
 *
 * @param <T> The message payload type
 * @author Chris Bono
 */
@FunctionalInterface
public interface PulsarReaderReaderBuilderCustomizer<T> {

	/**
	 * Customize the {@link ReaderBuilder}.
	 * @param readerBuilder the builder to customize
	 */
	void customize(ReaderBuilder<T> readerBuilder);

}
