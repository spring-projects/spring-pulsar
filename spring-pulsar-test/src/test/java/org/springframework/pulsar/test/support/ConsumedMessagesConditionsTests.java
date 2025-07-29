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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.springframework.pulsar.test.support.ConsumedMessagesConditions.desiredMessageCount;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link ConsumedMessagesConditions}.
 *
 * @author Jonas Geiregat
 */
class ConsumedMessagesConditionsTests {

	private List<Message<String>> createStringMessages(int count) {
		return IntStream.range(0, count)
			.<Message<String>>mapToObj(i -> MessageImpl.create(new MessageMetadata(),
					ByteBuffer.wrap(("message-" + i).getBytes()), Schema.STRING, "topic"))
			.toList();
	}

	@Nested
	class DesiredMessageCount {

		@Test
		void receivedMessageCountMeetCondition() {
			ConsumedMessagesCondition<String> condition = desiredMessageCount(2);
			var messages = createStringMessages(2);
			assertThat(condition.meets(messages)).isTrue();
		}

		@Test
		void receivedMessageCountDoesNotMeetCondition() {
			ConsumedMessagesCondition<String> condition = desiredMessageCount(3);
			var messages = createStringMessages(2);
			assertThat(condition.meets(messages)).isFalse();
		}

		@ParameterizedTest
		@ValueSource(ints = { 0, -1 })
		void throwExceptionWhenDesiredMessageCountEqualOrLessThanZero(int messageCount) {
			assertThatIllegalStateException().isThrownBy(() -> desiredMessageCount(messageCount))
				.withMessage("Desired message count must be greater than 0");
		}

	}

	@Nested
	class AtLeaseOneMessageMatches {

		@Test
		void messageValuesContainsExpectation() {
			ConsumedMessagesCondition<String> condition = ConsumedMessagesConditions
				.atLeastOneMessageMatches("message-1");
			var messages = createStringMessages(3);
			assertThat(condition.meets(messages)).isTrue();
		}

		@Test
		void messageValuesDoesNotContainExpectation() {
			ConsumedMessagesCondition<String> condition = ConsumedMessagesConditions
				.atLeastOneMessageMatches("message-5");
			var messages = createStringMessages(3);
			assertThat(condition.meets(messages)).isFalse();
		}

	}

	@Nested
	class AtLeaseOneMessageMatchesEachOf {

		@Test
		void messageValuesContainsExpectation() {
			ConsumedMessagesCondition<String> condition = ConsumedMessagesConditions
				.atLeastOneMessageMatchesEachOf("message-1", "message-2");
			var messages = createStringMessages(3);
			assertThat(condition.meets(messages)).isTrue();
		}

		@Test
		void messageValuesDoesNotContainExpectation() {
			ConsumedMessagesCondition<String> condition = ConsumedMessagesConditions
				.atLeastOneMessageMatchesEachOf("message-3", "message-4");
			var messages = createStringMessages(3);
			assertThat(condition.meets(messages)).isFalse();
		}

	}

}
