/*
 * Copyright 2022-2023 the original author or authors.
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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.pulsar.client.api.Message;

import org.springframework.messaging.MessageHeaders;

/**
 * Default implementation of {@link PulsarHeaderMapper}.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public class DefaultPulsarHeaderMapper implements PulsarHeaderMapper {

	@Override
	public Map<String, String> fromSpringHeaders(MessageHeaders springHeaders) {
		Objects.requireNonNull(springHeaders, "springHeaders must be specified");
		var pulsarHeaders = new LinkedHashMap<String, String>();
		springHeaders.forEach((k, v) -> pulsarHeaders.put(k, Objects.toString(v, null)));
		return pulsarHeaders;
	}

	@Override
	public MessageHeaders toSpringHeaders(Message<?> pulsarMessage) {
		Objects.requireNonNull(pulsarMessage, "pulsarMessage must be specified");
		var headersMap = new HashMap<String, Object>(pulsarMessage.getProperties());
		if (pulsarMessage.hasKey()) {
			headersMap.put(PulsarHeaders.KEY, pulsarMessage.getKey());
			headersMap.put(PulsarHeaders.KEY_BYTES, pulsarMessage.getKeyBytes());
		}
		if (pulsarMessage.hasOrderingKey()) {
			headersMap.put(PulsarHeaders.ORDERING_KEY, pulsarMessage.getOrderingKey());
		}
		if (pulsarMessage.hasIndex()) {
			headersMap.put(PulsarHeaders.INDEX, pulsarMessage.getIndex());
		}
		headersMap.put(PulsarHeaders.MESSAGE_ID, pulsarMessage.getMessageId());
		headersMap.put(PulsarHeaders.BROKER_PUBLISH_TIME, pulsarMessage.getBrokerPublishTime());
		headersMap.put(PulsarHeaders.EVENT_TIME, pulsarMessage.getEventTime());
		headersMap.put(PulsarHeaders.MESSAGE_SIZE, pulsarMessage.size());
		headersMap.put(PulsarHeaders.PRODUCER_NAME, pulsarMessage.getProducerName());
		headersMap.put(PulsarHeaders.RAW_DATA, pulsarMessage.getData());
		headersMap.put(PulsarHeaders.PUBLISH_TIME, pulsarMessage.getPublishTime());
		headersMap.put(PulsarHeaders.REDELIVERY_COUNT, pulsarMessage.getRedeliveryCount());
		headersMap.put(PulsarHeaders.REPLICATED_FROM, pulsarMessage.getReplicatedFrom());
		headersMap.put(PulsarHeaders.SCHEMA_VERSION, pulsarMessage.getSchemaVersion());
		headersMap.put(PulsarHeaders.SEQUENCE_ID, pulsarMessage.getSequenceId());
		headersMap.put(PulsarHeaders.TOPIC_NAME, pulsarMessage.getTopicName());
		return new MessageHeaders(headersMap);
	}

}
