/*
 * Copyright 2024 the original author or authors.
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
 * Assertions related step in the fluent API for building a Pulsar test consumer.
 *
 * @param <T> the type of the message payload
 * @author Jonas Geiregat
 */
public interface ConditionsStep<T> {

	/**
	 * The maximum timeout duration to wait for the desired number of messages to be
	 * reached.
	 * @param timeout the maximum timeout duration to wait
	 * @return the next step in the fluent API
	 */
	ConditionsStep<T> awaitAtMost(Duration timeout);

	/**
	 * Start consuming until the given condition is met.
	 * @param condition the condition to be met
	 * @return the next step in the fluent API
	 */
	List<Message<T>> until(Condition<T> condition);

}
