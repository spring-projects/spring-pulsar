/*
 * Copyright 2024-2024 the original author or authors.
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

package org.springframework.pulsar.transaction;

import org.apache.pulsar.client.api.PulsarClient;

import org.springframework.transaction.PlatformTransactionManager;

/**
 * A transaction manager that can provide a {@link PulsarClient}.
 *
 * @author Chris Bono
 * @since 1.1.0
 */
public interface PulsarAwareTransactionManager extends PlatformTransactionManager {

	/**
	 * Get the Pulsar client.
	 * @return the Pulsar client
	 */
	PulsarClient getPulsarClient();

}
