/*
 * Copyright 2023 the original author or authors.
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

package org.springframework.pulsar.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.AssertionsForClassTypes.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.messaging.MessageHeaders;

/**
 * Tests for {@link DefaultPulsarHeaderMapper}.
 *
 * @author Chris Bono
 */
class DefaultPulsarHeaderMapperTests {

	private DefaultPulsarHeaderMapper mapper = new DefaultPulsarHeaderMapper();

	@SuppressWarnings("unchecked")
	private Message<String> mockPulsarMessage(boolean includeOptionalMetadata, Map<String, String> userProperties) {
		Message<String> pulsarMessage = (Message<String>) mock(Message.class);

		// custom user properties
		when(pulsarMessage.getProperties()).thenReturn(userProperties);

		// optional metadata
		when(pulsarMessage.hasKey()).thenReturn(includeOptionalMetadata);
		when(pulsarMessage.getKey()).thenReturn("key");
		when(pulsarMessage.getKeyBytes()).thenReturn("key".getBytes());
		when(pulsarMessage.hasOrderingKey()).thenReturn(includeOptionalMetadata);
		when(pulsarMessage.getOrderingKey()).thenReturn("orderingKey".getBytes());
		when(pulsarMessage.hasIndex()).thenReturn(includeOptionalMetadata);
		when(pulsarMessage.getIndex()).thenReturn(Optional.of(1L));

		// required metadata
		MessageId messageId = mock(MessageId.class);
		when(pulsarMessage.getMessageId()).thenReturn(messageId);
		when(pulsarMessage.getBrokerPublishTime()).thenReturn(Optional.of(100L));
		when(pulsarMessage.getEventTime()).thenReturn(200L);
		when(pulsarMessage.size()).thenReturn(300);
		when(pulsarMessage.getProducerName()).thenReturn("producerName");
		when(pulsarMessage.getData()).thenReturn("data".getBytes());
		when(pulsarMessage.getPublishTime()).thenReturn(400L);
		when(pulsarMessage.getRedeliveryCount()).thenReturn(500);
		when(pulsarMessage.getReplicatedFrom()).thenReturn("replicatedFrom");
		when(pulsarMessage.getSchemaVersion()).thenReturn("schemaVersion".getBytes());
		when(pulsarMessage.getSequenceId()).thenReturn(600L);
		when(pulsarMessage.getTopicName()).thenReturn("topicName");

		return pulsarMessage;
	}

	@Nested
	class FromSpringHeaders {

		@Test
		void nullSpringHeaders() {
			assertThatNullPointerException().isThrownBy(() -> mapper.fromSpringHeaders(null))
					.withMessage("springHeaders must be specified");
		}

		@Test
		void emptySpringHeaders() {
			assertThat(mapper.fromSpringHeaders(new MessageHeaders(Collections.emptyMap()))).containsOnlyKeys("id",
					"timestamp");
		}

		@Test
		void springHeadersWithNullValue() {
			var headers = new HashMap<String, Object>();
			headers.put("foo", "bar");
			headers.put("uuid", null);
			assertThat(mapper.fromSpringHeaders(new MessageHeaders(headers)))
					.containsOnlyKeys("id", "timestamp", "foo", "uuid")
					.contains(entry("foo", "bar"), entry("uuid", null));
		}

		@Test
		void springHeadersWithValues() {
			var uuid = UUID.randomUUID();
			var headers = new HashMap<String, Object>();
			headers.put("foo", "bar");
			headers.put("uuid", uuid);
			assertThat(mapper.fromSpringHeaders(new MessageHeaders(headers)))
					.containsOnlyKeys("id", "timestamp", "foo", "uuid")
					.contains(entry("foo", "bar"), entry("uuid", uuid.toString()));
		}

	}

	@Nested
	class ToSpringHeaders {

		@Test
		void nullPulsarMessage() {
			assertThatNullPointerException().isThrownBy(() -> mapper.toSpringHeaders(null))
					.withMessage("pulsarMessage must be specified");
		}

		@Test
		void pulsarMessageWithOnlyRequiredMetadata() {
			var pulsarMessage = mockPulsarMessage(false, Collections.emptyMap());
			var springHeaders = mapper.toSpringHeaders(pulsarMessage);
			// @formatter:off
			assertThat(springHeaders).contains(
					entry(PulsarHeaders.MESSAGE_ID, pulsarMessage.getMessageId()),
					entry(PulsarHeaders.BROKER_PUBLISH_TIME, pulsarMessage.getBrokerPublishTime()),
					entry(PulsarHeaders.EVENT_TIME, pulsarMessage.getEventTime()),
					entry(PulsarHeaders.MESSAGE_SIZE, pulsarMessage.size()),
					entry(PulsarHeaders.PRODUCER_NAME, pulsarMessage.getProducerName()),
					entry(PulsarHeaders.RAW_DATA, pulsarMessage.getData()),
					entry(PulsarHeaders.PUBLISH_TIME, pulsarMessage.getPublishTime()),
					entry(PulsarHeaders.REDELIVERY_COUNT, pulsarMessage.getRedeliveryCount()),
					entry(PulsarHeaders.REPLICATED_FROM, pulsarMessage.getReplicatedFrom()),
					entry(PulsarHeaders.SCHEMA_VERSION, pulsarMessage.getSchemaVersion()),
					entry(PulsarHeaders.SEQUENCE_ID, pulsarMessage.getSequenceId()),
					entry(PulsarHeaders.TOPIC_NAME, pulsarMessage.getTopicName()));
			assertThat(springHeaders).doesNotContainKeys(
					PulsarHeaders.KEY,
					PulsarHeaders.KEY_BYTES,
					PulsarHeaders.ORDERING_KEY,
					PulsarHeaders.INDEX);
			// @formatter:on
		}

		@Test
		void pulsarMessageWithAllMetadataAndUserProperties() {
			var uuid = UUID.randomUUID();
			var customHeaders = new HashMap<String, String>();
			customHeaders.put("foo", "bar");
			customHeaders.put("uuid", uuid.toString());
			var pulsarMessage = mockPulsarMessage(true, customHeaders);
			var springHeaders = mapper.toSpringHeaders(pulsarMessage);
			// @formatter:off
			assertThat(springHeaders).contains(
					entry("foo", "bar"),
					entry("uuid", uuid.toString()),
					entry(PulsarHeaders.KEY, pulsarMessage.getKey()),
					entry(PulsarHeaders.KEY_BYTES, pulsarMessage.getKeyBytes()),
					entry(PulsarHeaders.ORDERING_KEY, pulsarMessage.getOrderingKey()),
					entry(PulsarHeaders.INDEX, pulsarMessage.getIndex()),
					entry(PulsarHeaders.MESSAGE_ID, pulsarMessage.getMessageId()),
					entry(PulsarHeaders.BROKER_PUBLISH_TIME, pulsarMessage.getBrokerPublishTime()),
					entry(PulsarHeaders.EVENT_TIME, pulsarMessage.getEventTime()),
					entry(PulsarHeaders.MESSAGE_SIZE, pulsarMessage.size()),
					entry(PulsarHeaders.PRODUCER_NAME, pulsarMessage.getProducerName()),
					entry(PulsarHeaders.RAW_DATA, pulsarMessage.getData()),
					entry(PulsarHeaders.PUBLISH_TIME, pulsarMessage.getPublishTime()),
					entry(PulsarHeaders.REDELIVERY_COUNT, pulsarMessage.getRedeliveryCount()),
					entry(PulsarHeaders.REPLICATED_FROM, pulsarMessage.getReplicatedFrom()),
					entry(PulsarHeaders.SCHEMA_VERSION, pulsarMessage.getSchemaVersion()),
					entry(PulsarHeaders.SEQUENCE_ID, pulsarMessage.getSequenceId()),
					entry(PulsarHeaders.TOPIC_NAME, pulsarMessage.getTopicName()));
			// @formatter:on
		}

	}

	@Nested
	class ToAndFromSpringHeaders {

		@Test
		void pulsarMessageWithOnlyRequiredMetadataRoundTripped() {
			var pulsarMessage = mockPulsarMessage(false, Collections.emptyMap());
			var springHeaders = mapper.toSpringHeaders(pulsarMessage);
			var pulsarHeaders = mapper.fromSpringHeaders(springHeaders);
			// @formatter:off
			assertThat(pulsarHeaders).contains(
					entry(PulsarHeaders.MESSAGE_ID, Objects.toString(pulsarMessage.getMessageId())),
					entry(PulsarHeaders.BROKER_PUBLISH_TIME, Objects.toString(pulsarMessage.getBrokerPublishTime())),
					entry(PulsarHeaders.EVENT_TIME, Objects.toString(pulsarMessage.getEventTime())),
					entry(PulsarHeaders.MESSAGE_SIZE, Objects.toString(pulsarMessage.size())),
					entry(PulsarHeaders.PRODUCER_NAME, pulsarMessage.getProducerName()),
					entry(PulsarHeaders.RAW_DATA, Objects.toString(pulsarMessage.getData())),
					entry(PulsarHeaders.PUBLISH_TIME, Objects.toString(pulsarMessage.getPublishTime())),
					entry(PulsarHeaders.REDELIVERY_COUNT, Objects.toString(pulsarMessage.getRedeliveryCount())),
					entry(PulsarHeaders.REPLICATED_FROM, pulsarMessage.getReplicatedFrom()),
					entry(PulsarHeaders.SCHEMA_VERSION, Objects.toString(pulsarMessage.getSchemaVersion())),
					entry(PulsarHeaders.SEQUENCE_ID, Objects.toString(pulsarMessage.getSequenceId())),
					entry(PulsarHeaders.TOPIC_NAME, pulsarMessage.getTopicName()));
			// @formatter:on
		}

		@Test
		void pulsarMessageWithAllMetadataAndUserPropertiesRoundTripped() {
			var pulsarMessage = mockPulsarMessage(true, Collections.singletonMap("foo", "bar"));
			var springHeaders = mapper.toSpringHeaders(pulsarMessage);
			var pulsarHeaders = mapper.fromSpringHeaders(springHeaders);
			// @formatter:off
			assertThat(pulsarHeaders).contains(
					entry("foo", "bar"),
					entry(PulsarHeaders.KEY, pulsarMessage.getKey()),
					entry(PulsarHeaders.KEY_BYTES, Objects.toString(pulsarMessage.getKeyBytes())),
					entry(PulsarHeaders.ORDERING_KEY, Objects.toString(pulsarMessage.getOrderingKey())),
					entry(PulsarHeaders.INDEX, Objects.toString(pulsarMessage.getIndex())),
					entry(PulsarHeaders.MESSAGE_ID, Objects.toString(pulsarMessage.getMessageId())),
					entry(PulsarHeaders.BROKER_PUBLISH_TIME, Objects.toString(pulsarMessage.getBrokerPublishTime())),
					entry(PulsarHeaders.EVENT_TIME, Objects.toString(pulsarMessage.getEventTime())),
					entry(PulsarHeaders.MESSAGE_SIZE, Objects.toString(pulsarMessage.size())),
					entry(PulsarHeaders.PRODUCER_NAME, pulsarMessage.getProducerName()),
					entry(PulsarHeaders.RAW_DATA, Objects.toString(pulsarMessage.getData())),
					entry(PulsarHeaders.PUBLISH_TIME, Objects.toString(pulsarMessage.getPublishTime())),
					entry(PulsarHeaders.REDELIVERY_COUNT, Objects.toString(pulsarMessage.getRedeliveryCount())),
					entry(PulsarHeaders.REPLICATED_FROM, pulsarMessage.getReplicatedFrom()),
					entry(PulsarHeaders.SCHEMA_VERSION, Objects.toString(pulsarMessage.getSchemaVersion())),
					entry(PulsarHeaders.SEQUENCE_ID, Objects.toString(pulsarMessage.getSequenceId())),
					entry(PulsarHeaders.TOPIC_NAME, pulsarMessage.getTopicName()));
			// @formatter:on
		}

	}

}
