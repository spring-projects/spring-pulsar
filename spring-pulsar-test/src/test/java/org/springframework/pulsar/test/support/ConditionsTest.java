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

import static org.springframework.pulsar.test.support.Conditions.desiredMessageCount;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ConditionsTest {

	@Nested
	class DesiredMessageCount {

		@Test
		void receivedMessageCountMeetCondition() {
			Condition<String> condition = desiredMessageCount(2);

			boolean result = condition.meets(List.of(
					MessageImpl.create(new MessageMetadata(), ByteBuffer.wrap("message-0".getBytes()), Schema.STRING,
							"topic"),
					MessageImpl.create(new MessageMetadata(), ByteBuffer.wrap("message-1".getBytes()), Schema.STRING,
							"topic")));

			Assertions.assertThat(result).isTrue();
		}

		@Test
		void receivedMessageCountDoesNotMeetCondition() {
			Condition<String> condition = desiredMessageCount(3);

			boolean result = condition.meets(List.of(
					MessageImpl.create(new MessageMetadata(), ByteBuffer.wrap("message-0".getBytes()), Schema.STRING,
							"topic"),
					MessageImpl.create(new MessageMetadata(), ByteBuffer.wrap("message-1".getBytes()), Schema.STRING,
							"topic")));

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
	class ContainsValueCondition {

		@Test
		void messageValueContainsExpectation() {
			Condition<String> condition = Conditions.containsValue("message-1");

			boolean result = condition.meets(List.of(
					MessageImpl.create(new MessageMetadata(), ByteBuffer.wrap("message-0".getBytes()), Schema.STRING,
							"topic"),
					MessageImpl.create(new MessageMetadata(), ByteBuffer.wrap("message-1".getBytes()), Schema.STRING,
							"topic"),
					MessageImpl.create(new MessageMetadata(), ByteBuffer.wrap("message-2".getBytes()), Schema.STRING,
							"topic")));

			Assertions.assertThat(result).isTrue();
		}

		@Test
		void messageValueDoesNotContainExpectation() {
			Condition<String> condition = Conditions.containsValue("message-3");

			boolean result = condition.meets(List.of(
					MessageImpl.create(new MessageMetadata(), ByteBuffer.wrap("message-0".getBytes()), Schema.STRING,
							"topic"),
					MessageImpl.create(new MessageMetadata(), ByteBuffer.wrap("message-1".getBytes()), Schema.STRING,
							"topic"),
					MessageImpl.create(new MessageMetadata(), ByteBuffer.wrap("message-2".getBytes()), Schema.STRING,
							"topic")));

			Assertions.assertThat(result).isFalse();
		}

	}

	@Nested
	class ContainsValuesCondition {

		@Test
		void messageValueContainsExpectation() {
			Condition<String> condition = Conditions.containsValues("message-1", "message-2");

			boolean result = condition.meets(List.of(
					MessageImpl.create(new MessageMetadata(), ByteBuffer.wrap("message-0".getBytes()), Schema.STRING,
							"topic"),
					MessageImpl.create(new MessageMetadata(), ByteBuffer.wrap("message-1".getBytes()), Schema.STRING,
							"topic"),
					MessageImpl.create(new MessageMetadata(), ByteBuffer.wrap("message-2".getBytes()), Schema.STRING,
							"topic")));

			Assertions.assertThat(result).isTrue();
		}

		@Test
		void messageValueDoesNotContainExpectation() {
			Condition<String> condition = Conditions.containsValues("message-3", "message-4");

			boolean result = condition.meets(List.of(
					MessageImpl.create(new MessageMetadata(), ByteBuffer.wrap("message-0".getBytes()), Schema.STRING,
							"topic"),
					MessageImpl.create(new MessageMetadata(), ByteBuffer.wrap("message-1".getBytes()), Schema.STRING,
							"topic"),
					MessageImpl.create(new MessageMetadata(), ByteBuffer.wrap("message-2".getBytes()), Schema.STRING,
							"topic")));

			Assertions.assertThat(result).isFalse();
		}

	}

}
