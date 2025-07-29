/*
 * Copyright 2023-present the original author or authors.
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

import org.apache.pulsar.client.api.ReaderBuilder;

/**
 * Callback interface that can be implemented to customize the {@link ReaderBuilder} that
 * is used by the {@link PulsarReaderFactory} to create readers.
 * <p>
 * When using Spring Boot autoconfiguration, any beans implementing this interface will be
 * used as default configuration by the {@link DefaultPulsarReaderFactory} and as such
 * will apply to all created readers.
 * <p>
 * The reader factory also supports passing in a specific instance of this callback when
 * {@link PulsarReaderFactory#createReader creating a reader} and as such the passed in
 * customizer only applies to the single created reader.
 *
 * @param <T> The message payload type
 * @author Soby Chacko
 */
@FunctionalInterface
public interface ReaderBuilderCustomizer<T> {

	/**
	 * Customizes a {@link ReaderBuilder}.
	 * @param readerBuilder the builder to customize
	 */
	void customize(ReaderBuilder<T> readerBuilder);

}
