/*
 * Copyright 2024-present the original author or authors.
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

package org.springframework.pulsar.test.support;

import java.time.Duration;
import java.util.List;

import org.apache.pulsar.client.api.Message;

/**
 * Conditions related step in the fluent API for building a Pulsar test consumer.
 *
 * @param <T> the type of the message payload
 * @author Jonas Geiregat
 */
public interface ConditionsSpec<T> {

	/**
	 * The maximum amount of time to consume messages and wait for the condition to be
	 * satisfied.
	 * @param timeout the maximum amount of time for the condition to be met
	 * @return the next step in the fluent API
	 */
	ConditionsSpec<T> awaitAtMost(Duration timeout);

	/**
	 * Consume messages until the condition is satisfied.
	 * @param condition the condition to be met
	 * @return the next step in the fluent API
	 */
	ConditionsSpec<T> until(ConsumedMessagesCondition<T> condition);

	/**
	 * Terminal operation that begins the message consumption using the configured specs.
	 * @return the consumed messages
	 * @throws ConditionTimeoutException if the condition is not met within the timeout
	 */
	List<Message<T>> get();

}
