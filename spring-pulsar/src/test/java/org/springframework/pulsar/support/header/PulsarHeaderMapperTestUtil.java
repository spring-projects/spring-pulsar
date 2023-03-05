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

package org.springframework.pulsar.support.header;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.pulsar.support.header.JsonPulsarHeaderMapper.JSON_TYPES;

import java.util.Map;
import java.util.Optional;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;

import org.springframework.messaging.MessageHeaders;
import org.springframework.pulsar.support.PulsarHeaders;

/**
 * Utilities for testing header mapper related functionality.
 *
 * @author Chris Bono
 */
final class PulsarHeaderMapperTestUtil {

	private PulsarHeaderMapperTestUtil() {
	}

	@SuppressWarnings("unchecked")
	static Message<String> mockPulsarMessage(boolean includeOptionalMetadata, Map<String, String> userProperties) {
		Message<String> pulsarMessage = (Message<String>) mock(Message.class);

		// custom user properties
		when(pulsarMessage.getProperties()).thenReturn(userProperties);
		when(pulsarMessage.hasProperty(JSON_TYPES)).thenReturn(userProperties.containsKey(JSON_TYPES));
		when(pulsarMessage.getProperty(JSON_TYPES)).thenReturn(userProperties.get(JSON_TYPES));

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

	static void assertSpringHeadersHavePulsarMetadata(MessageHeaders springHeaders, Message<String> pulsarMessage,
			boolean shouldHaveOptionalFields) {
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
		if (shouldHaveOptionalFields) {
			assertThat(springHeaders).contains(
					entry(PulsarHeaders.KEY, pulsarMessage.getKey()),
					entry(PulsarHeaders.KEY_BYTES, pulsarMessage.getKeyBytes()),
					entry(PulsarHeaders.ORDERING_KEY, pulsarMessage.getOrderingKey()),
					entry(PulsarHeaders.INDEX, pulsarMessage.getIndex()));
		}
		else {
			assertThat(springHeaders).doesNotContainKeys(
					PulsarHeaders.KEY,
					PulsarHeaders.KEY_BYTES,
					PulsarHeaders.ORDERING_KEY,
					PulsarHeaders.INDEX);
		}
	}

}
