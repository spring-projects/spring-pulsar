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

package org.springframework.pulsar.config;

import java.util.Optional;

/**
 * Contract to generate a subscription name for the Pulsar consumer endpoints.
 *
 * @param <T> the type of endpoint annotation - one of {@code PulsarListener} or
 * {@code ReactivePulsarListener}
 * @author Chris Bono
 */
public interface PulsarEndpointSubscriptionNameGenerator<T> {

	/**
	 * Generates a subscription name for the specified endpoint.
	 * @param endpointSpecifier the annotation on the endpoint to generate the name for
	 * @return the generated name or empty to have the container use its default name
	 * generation strategy
	 */
	Optional<String> generateName(T endpointSpecifier);

}
