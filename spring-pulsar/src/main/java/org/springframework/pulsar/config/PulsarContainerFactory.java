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

/**
 * Factory for Pulsar message listener containers.
 *
 * @param <C> message container
 * @param <E> message listener endpoint
 * @author Chris Bono
 * @since 1.2.0
 */
public interface PulsarContainerFactory<C, E> {

	/**
	 * Create a message listener container for the given endpoint. Containers created
	 * using this method are added to the listener endpoint registry.
	 * @param endpoint the endpoint to configure
	 * @return the created container
	 */
	C createRegisteredContainer(E endpoint);

	/**
	 * Create and configure a container without a listener. Containers created using this
	 * method are not added to the listener endpoint registry.
	 * @param topics the topics.
	 * @return the container.
	 */
	C createContainer(String... topics);

}
