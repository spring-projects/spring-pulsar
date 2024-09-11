/*
 * Copyright 2022-2024 the original author or authors.
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
 * Callback interface that can be implemented to customize a
 * {@link ConcurrentPulsarListenerContainerFactory}.
 *
 * @param <T> The message payload type
 * @author Chris Bono
 * @deprecated since 1.2.0 for removal in 1.4.0 in favor of
 * {@code org.springframework.boot.autoconfigure.pulsar.PulsarContainerFactoryCustomizer<ConcurrentPulsarListenerContainerFactory<?>>}
 */
@FunctionalInterface
@Deprecated(since = "1.2.0", forRemoval = true)
public interface ConcurrentPulsarListenerContainerFactoryCustomizer<T> {

	/**
	 * Customize a {@link ConcurrentPulsarListenerContainerFactory}.
	 * @param containerFactory the factory to customize
	 */
	void customize(ConcurrentPulsarListenerContainerFactory<T> containerFactory);

}
