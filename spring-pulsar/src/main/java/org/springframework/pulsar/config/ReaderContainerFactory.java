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

package org.springframework.pulsar.config;

import org.springframework.pulsar.reader.PulsarMessageReaderContainer;

/**
 * Base container factory interface for {@link PulsarMessageReaderContainer}.
 *
 * @param <C> Container type
 * @param <E> Endpoint type
 * @author Soby Chacko
 * @author Chris Bono
 */
public interface ReaderContainerFactory<C extends PulsarMessageReaderContainer, E extends PulsarReaderEndpoint<C>>
		extends PulsarContainerFactory<C, E> {

	/**
	 * Create a message reader container for the given endpoint and register the container
	 * with the listener endpoint registry.
	 * @param endpoint reader endpoint
	 * @return the created container
	 * @deprecated since 1.2.0 for removal in 1.4.0 in favor of
	 * {@link PulsarContainerFactory#createRegisteredContainer}
	 */
	@Deprecated(since = "1.2.0", forRemoval = true)
	default C createReaderContainer(E endpoint) {
		return createRegisteredContainer(endpoint);
	}

	/**
	 * Create a message reader container for the given endpoint.
	 * @param topics the topics to read from
	 * @return the created container
	 * @deprecated since 1.2.0 for removal in 1.4.0 in favor of
	 * {@link PulsarContainerFactory#createContainer}
	 */
	@Deprecated(since = "1.2.0", forRemoval = true)
	default C createReaderContainer(String... topics) {
		return createContainer(topics);
	}

}
