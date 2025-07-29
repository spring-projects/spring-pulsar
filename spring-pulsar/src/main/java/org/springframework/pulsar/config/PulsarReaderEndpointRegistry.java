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
 * Creates the necessary {@link PulsarMessageReaderContainer} instances for the registered
 * {@linkplain PulsarReaderEndpoint endpoints}. Also manages the lifecycle of the listener
 * containers, in particular within the lifecycle of the application context.
 *
 * <p>
 * Contrary to {@link PulsarMessageReaderContainer}s created manually, listener containers
 * managed by registry are not beans in the application context and are not candidates for
 * autowiring. Use {@link #getReaderContainer(String)} ()} if you need to access this
 * registry's listener containers for management purposes. If you need to access to a
 * specific message listener container, use {@link #getReaderContainer(String)} with the
 * id of the endpoint.
 *
 * @author Soby Chacko
 */
public class PulsarReaderEndpointRegistry extends
		GenericReaderEndpointRegistry<PulsarMessageReaderContainer, PulsarReaderEndpoint<PulsarMessageReaderContainer>> {

	public PulsarReaderEndpointRegistry() {
		super(PulsarMessageReaderContainer.class);
	}

}
