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

import static org.springframework.pulsar.test.support.ConsumedMessagesConditions.desiredMessageCount;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link ConsumedMessagesConditions}.
 *
 * @author Jonas Geiregat
 */
class ConsumedMessagesConditionsTest {

	private List<Message<String>> createStringMessages(int count) {
		return IntStream.range(0, count)
			.<Message<String>>mapToObj(i -> MessageImpl.create(new MessageMetadata(),
					ByteBuffer.wrap(("message-" + i).getBytes()), Schema.STRING, "topic"))
			.toList();
	}

	@Nested
	class DesiredMessageCountTests {

		@Test
		void receivedMessageCountMeetCondition() {
			ConsumedMessagesCondition<String> consumedMessagesCondition = desiredMessageCount(2);

			boolean result = consumedMessagesCondition.meets(createStringMessages(2));

			Assertions.assertThat(result).isTrue();
		}

		@Test
		void receivedMessageCountDoesNotMeetCondition() {
			ConsumedMessagesCondition<String> consumedMessagesCondition = desiredMessageCount(3);

			boolean result = consumedMessagesCondition.meets(createStringMessages(2));

			Assertions.assertThat(result).isFalse();
		}

		@ParameterizedTest
		@ValueSource(ints = { 0, -1 })
		void throwExceptionWhenDesiredMessageCountEqualOrLessThanZero(int messageCount) {
			Assertions.assertThatThrownBy(() -> desiredMessageCount(messageCount))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("Desired message count must be greater than 0");
		}

	}

	@Nested
	class AnyMessageMatchesExpectedConditionTests {

		@Test
		void messageValuesContainsExpectation() {
			ConsumedMessagesCondition<String> consumedMessagesCondition = ConsumedMessagesConditions
				.anyMessageMatchesExpected("message-1");

			boolean result = consumedMessagesCondition.meets(createStringMessages(3));

			Assertions.assertThat(result).isTrue();
		}

		@Test
		void messageValuesDoesNotContainExpectation() {
			ConsumedMessagesCondition<String> consumedMessagesCondition = ConsumedMessagesConditions
				.anyMessageMatchesExpected("message-3");

			boolean result = consumedMessagesCondition.meets(createStringMessages(3));

			Assertions.assertThat(result).isFalse();
		}

	}

	@Nested
	class ContainsAllExpectedValuesConditionTests {

		@Test
		void messageValuesContainsExpectation() {
			ConsumedMessagesCondition<String> consumedMessagesCondition = ConsumedMessagesConditions
				.containsAllExpectedValues("message-1", "message-2");

			boolean result = consumedMessagesCondition.meets(createStringMessages(3));

			Assertions.assertThat(result).isTrue();
		}

		@Test
		void messageValuesDoesNotContainExpectation() {
			ConsumedMessagesCondition<String> consumedMessagesCondition = ConsumedMessagesConditions
				.containsAllExpectedValues("message-3", "message-4");

			boolean result = consumedMessagesCondition.meets(createStringMessages(3));

			Assertions.assertThat(result).isFalse();
		}

	}

	@Nested
	class ContainsExactlyExpectedValuesConditionTests {

		@Test
		void messageValuesContainsExpectations() {
			ConsumedMessagesCondition<String> consumedMessagesCondition = ConsumedMessagesConditions
				.containsExactlyExpectedValues("message-0", "message-1");

			boolean result = consumedMessagesCondition.meets(createStringMessages(2));

			Assertions.assertThat(result).isTrue();
		}

		@Test
		void messageValuesDoesNotContainExpectation() {
			ConsumedMessagesCondition<String> consumedMessagesCondition = ConsumedMessagesConditions
				.containsExactlyExpectedValues("message-0", "message-1");

			boolean result = consumedMessagesCondition.meets(createStringMessages(3));

			Assertions.assertThat(result).isFalse();
		}

	}

}
