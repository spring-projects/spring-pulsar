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

import java.util.stream.Stream;

import org.apache.pulsar.client.api.Message;

import org.springframework.util.Assert;

/**
 * Exposes a set of commonly used conditions to be used in {@link PulsarConsumerTestUtil}.
 *
 * @author Jonas Geiregat
 */
public interface Conditions {

	/**
	 * Verifies that the consumed messages contain the expected message count.
	 * @param messageCount the desired message count
	 * @param <T> the type of the message
	 * @return the condition
	 */
	static <T> ConsumedMessagesCondition<T> desiredMessageCount(int messageCount) {
		Assert.state(messageCount > 0, "Desired message count must be greater than 0");
		return messages -> messages.size() == messageCount;
	}

	/**
	 * Verifies that at least one of the consumed messages has a payload that equals the
	 * specified value.
	 * @param expectation the expected value
	 * @param <T> the type of the message
	 * @return the condition
	 */
	static <T> ConsumedMessagesCondition<T> containsValue(T expectation) {
		return messages -> messages.stream().anyMatch(message -> message.getValue().equals(expectation));
	}

	/**
	 * Verifies that the consumed messages value contains at least all expected values.
	 * @param expectation the expected value
	 * @param <T> the type of the message
	 * @return the condition
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	static <T> ConsumedMessagesCondition<T> containsValues(T... expectation) {
		return messages -> {
			var values = messages.stream().map(Message::getValue).toList();
			return Stream.of(expectation).allMatch(values::contains);
		};
	}

}
