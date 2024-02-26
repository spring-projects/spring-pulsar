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

import java.util.List;

import org.apache.pulsar.client.api.Message;

/**
 * A condition to be used in {@link PulsarConsumerTestUtil} to verify if the consumed
 * messages satisfy the given criteria.
 *
 * @param <T> the type of the message
 * @author Jonas Geiregat
 */
@FunctionalInterface
public interface ConsumedMessagesCondition<T> {

	/**
	 * Determines if the consumed messages meets the condition.
	 * @param messages the consumed messages
	 * @return whether the consumed messages meet the condition
	 */
	boolean meets(List<Message<T>> messages);

	default ConsumedMessagesCondition<T> and(ConsumedMessagesCondition<T> other) {
		return messages -> meets(messages) && other.meets(messages);
	}

}
