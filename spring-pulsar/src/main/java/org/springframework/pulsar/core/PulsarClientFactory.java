/*
 * Copyright 2023-2024 the original author or authors.
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

import org.apache.pulsar.client.api.PulsarClient;

import org.springframework.pulsar.PulsarException;

/**
 * Pulsar client factory interface.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public interface PulsarClientFactory {

	/**
	 * Create a client.
	 * @return the created client instance
	 * @throws PulsarException if an error occurs creating the client
	 */
	PulsarClient createClient();

}
