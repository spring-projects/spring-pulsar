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

package org.springframework.pulsar.config;

import org.springframework.pulsar.listener.PulsarMessageListenerContainer;

/**
 * Creates the necessary {@link PulsarMessageListenerContainer} instances for the
 * registered {@linkplain PulsarListenerEndpoint endpoints}. Also manages the lifecycle of
 * the listener containers, in particular within the lifecycle of the application context.
 *
 * <p>
 * Contrary to {@link PulsarMessageListenerContainer}s created manually, listener
 * containers managed by registry are not beans in the application context and are not
 * candidates for autowiring. Use {@link #getListenerContainers()} if you need to access
 * this registry's listener containers for management purposes. If you need to access to a
 * specific message listener container, use {@link #getListenerContainer(String)} with the
 * id of the endpoint.
 *
 * @author Soby Chacko
 * @author Christophe Bornet
 */
public class PulsarListenerEndpointRegistry
		extends GenericListenerEndpointRegistry<PulsarMessageListenerContainer, PulsarListenerEndpoint> {

	public PulsarListenerEndpointRegistry() {
		super(PulsarMessageListenerContainer.class);
	}

}
