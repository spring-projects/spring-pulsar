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

import java.util.stream.Stream;

import org.apache.pulsar.client.api.Message;

import org.springframework.util.Assert;

/**
 * A factory for creating commonly used {@link ConsumedMessagesCondition conditions} that
 * can be used with {@link PulsarConsumerTestUtil}.
 *
 * @author Jonas Geiregat
 */
public interface ConsumedMessagesConditions {

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
	 * Verifies that the expected value equals the message payload value of at least one
	 * consumed message.
	 * @param expectation the expected value
	 * @param <T> the type of the message
	 * @return the condition
	 */
	static <T> ConsumedMessagesCondition<T> atLeastOneMessageMatches(T expectation) {
		return messages -> messages.stream().map(Message::getValue).anyMatch(expectation::equals);
	}

	/**
	 * Verifies that each expected value equals the message payload value of at least one
	 * consumed message.
	 * @param expectation the expected values
	 * @param <T> the type of the message
	 * @return the condition
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	static <T> ConsumedMessagesCondition<T> atLeastOneMessageMatchesEachOf(T... expectation) {
		return messages -> {
			var values = messages.stream().map(Message::getValue).toList();
			return Stream.of(expectation).allMatch(values::contains);
		};
	}

}
