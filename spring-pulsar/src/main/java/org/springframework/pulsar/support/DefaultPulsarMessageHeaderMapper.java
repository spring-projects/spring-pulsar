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

import java.util.Map;

import org.apache.pulsar.client.api.Message;

/**
 * Implementation of {@link PulsarMessageHeaderMapper}.
 *
 * @author Soby Chacko
 */
public class DefaultPulsarMessageHeaderMapper implements PulsarMessageHeaderMapper {

	@Override
	public void toHeaders(Message<?> source, Map<String, Object> target) {
		target.putAll(source.getProperties());
		if (source.hasKey()) {
			target.put(PulsarHeaders.KEY, source.getKey());
			target.put(PulsarHeaders.KEY_BYTES, source.getKeyBytes());
		}
		if (source.hasOrderingKey()) {
			target.put(PulsarHeaders.ORDERING_KEY, source.getOrderingKey());
		}
		if (source.hasIndex()) {
			target.put(PulsarHeaders.INDEX, source.getIndex());
		}
		target.put(PulsarHeaders.MESSAGE_ID, source.getMessageId());
		target.put(PulsarHeaders.BROKER_PUBLISH_TIME, source.getBrokerPublishTime());
		target.put(PulsarHeaders.EVENT_TIME, source.getEventTime());
		target.put(PulsarHeaders.MESSAGE_SIZE, source.size());
		target.put(PulsarHeaders.PRODUCER_NAME, source.getProducerName());
		target.put(PulsarHeaders.RAW_DATA, source.getData());
		target.put(PulsarHeaders.PUBLISH_TIME, source.getPublishTime());
		target.put(PulsarHeaders.REDELIVERY_COUNT, source.getRedeliveryCount());
		target.put(PulsarHeaders.REPLICATED_FROM, source.getReplicatedFrom());
		target.put(PulsarHeaders.SCHEMA_VERSION, source.getSchemaVersion());
		target.put(PulsarHeaders.SEQUENCE_ID, source.getSequenceId());
		target.put(PulsarHeaders.TOPIC_NAME, source.getTopicName());
	}

}
