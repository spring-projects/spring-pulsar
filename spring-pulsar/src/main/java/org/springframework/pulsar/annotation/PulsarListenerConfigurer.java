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

package org.springframework.pulsar.annotation;

import org.springframework.pulsar.config.PulsarListenerContainerFactory;
import org.springframework.pulsar.config.PulsarListenerEndpoint;
import org.springframework.pulsar.config.PulsarListenerEndpointRegistrar;
import org.springframework.pulsar.config.PulsarListenerEndpointRegistry;

/**
 * Optional interface to be implemented by Spring managed bean willing to customize how
 * Pulsar listener endpoints are configured. Typically used to define the default
 * {@link PulsarListenerContainerFactory} to use or for registering Pulsar endpoints in a
 * <em>programmatic</em> fashion as opposed to the <em>declarative</em> approach of using
 * the {@link PulsarListener} annotation.
 *
 * @author Soby Chacko
 * @author Chris Bono
 * @see PulsarListenerEndpointRegistrar
 */
public interface PulsarListenerConfigurer {

	/**
	 * Callback allowing a {@link PulsarListenerEndpointRegistry} and specific
	 * {@link PulsarListenerEndpoint} instances to be registered against the given
	 * {@link PulsarListenerEndpointRegistrar}. The default
	 * {@link PulsarListenerContainerFactory} can also be customized.
	 * @param registrar the registrar to be configured
	 */
	void configurePulsarListeners(PulsarListenerEndpointRegistrar registrar);

}
