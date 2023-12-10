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

package org.springframework.pulsar.reactive.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.reactive.client.api.MessageResult;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import org.springframework.messaging.support.GenericMessage;
import org.springframework.pulsar.support.PulsarHeaders;

/**
 * Tests for {@link MessageUtils}.
 *
 * @author Chris Bono
 */
class MessageUtilsTests {

	@Nested
	class ExtractMessageIdApi {

		@Test
		void shouldReturnMessageIdWhenValidHeader() {
			var msgId = mock(MessageId.class);
			var msg = new GenericMessage<>("m1", Map.of(PulsarHeaders.MESSAGE_ID, msgId));
			assertThat(MessageUtils.extractMessageId(msg)).isEqualTo(msgId);
		}

		@Test
		void shouldThrowExceptionWhenInvalidHeader() {
			var msg = new GenericMessage<>("m1", Map.of(PulsarHeaders.MESSAGE_ID, "badId"));
			assertThatIllegalStateException().isThrownBy(() -> MessageUtils.extractMessageId(msg))
				.withMessage("Spring Message missing 'pulsar_message_id' header");
		}

		@Test
		void shouldThrowExceptionWhenEmptyHeaders() {
			var msg = new GenericMessage<>("m1");
			assertThatIllegalStateException().isThrownBy(() -> MessageUtils.extractMessageId(msg))
				.withMessage("Spring Message missing 'pulsar_message_id' header");
		}

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Nested
	class AcknowledgeApi {

		@Test
		void shouldDelegateToMessageResultAcknowledge() {
			var msgId = mock(MessageId.class);
			var msg = new GenericMessage<>("m1", Map.of(PulsarHeaders.MESSAGE_ID, msgId));
			try (MockedStatic<MessageResult> messageResult = mockStatic(MessageResult.class)) {
				var mockedReturnValue = (MessageResult<Void>) mock(MessageResult.class);
				when(MessageResult.acknowledge(any(MessageId.class))).thenReturn(mockedReturnValue);
				var returnedResult = MessageUtils.acknowledge(msg);
				assertThat(returnedResult).isEqualTo(mockedReturnValue);
				messageResult.verify(() -> MessageResult.acknowledge(msgId));
			}
		}

		@Test
		void shouldDelegateToMessageResultNegativeAcknowledge() {
			var msgId = mock(MessageId.class);
			var msg = new GenericMessage<>("m1", Map.of(PulsarHeaders.MESSAGE_ID, msgId));
			try (MockedStatic<MessageResult> messageResult = mockStatic(MessageResult.class)) {
				var mockedReturnValue = (MessageResult<Void>) mock(MessageResult.class);
				when(MessageResult.negativeAcknowledge(any(MessageId.class))).thenReturn(mockedReturnValue);
				var returnedResult = MessageUtils.negativeAcknowledge(msg);
				assertThat(returnedResult).isEqualTo(mockedReturnValue);
				messageResult.verify(() -> MessageResult.negativeAcknowledge(msgId));
			}
		}

	}

}
