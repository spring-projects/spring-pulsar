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

package org.springframework.pulsar.core;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;

import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

/**
 * Default implementation of {@link PulsarReaderFactory}.
 *
 * @param <T> message type
 * @author Soby Chacko
 */
public class DefaultPulsarReaderFactory<T> implements PulsarReaderFactory<T> {

	private final PulsarClient pulsarClient;

	private final Map<String, Object> readerConfig;

	public DefaultPulsarReaderFactory(PulsarClient pulsarClient) {
		this(pulsarClient, Collections.emptyMap());
	}

	public DefaultPulsarReaderFactory(PulsarClient pulsarClient, Map<String, Object> readerConfig) {
		this.pulsarClient = pulsarClient;
		this.readerConfig = readerConfig;
	}

	@Override
	public Reader<T> createReader(@Nullable List<String> topics, @Nullable MessageId messageId, Schema<T> schema)
			throws PulsarClientException {
		ReaderBuilder<T> readerBuilder = this.pulsarClient.newReader(schema);
		if (!CollectionUtils.isEmpty(topics)) {
			readerBuilder.topics(topics);
		}
		readerBuilder.startMessageId(messageId);

		readerBuilder.loadConf(this.readerConfig);
		return readerBuilder.create();
	}

}
