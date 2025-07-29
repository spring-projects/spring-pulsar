/*
 * Copyright 2023-present the original author or authors.
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

import java.util.List;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;

import org.springframework.lang.Nullable;
import org.springframework.pulsar.PulsarException;

/**
 * Pulsar {@link Reader} factory interface.
 *
 * @param <T> Underlying message type handled by this reader.
 * @author Soby Chacko
 * @author Chris Bono
 */
public interface PulsarReaderFactory<T> {

	/**
	 * Creating a Pulsar {@link Reader} based on the provided topics, schema and
	 * properties.
	 * @param topics set of topics to read from
	 * @param messageId starting message id to read from
	 * @param schema schema of the message to consume
	 * @param customizers the optional list of customizers to apply to the reader builder.
	 * Note that the customizers are applied last and have the potential for overriding
	 * any specified parameters or default properties.
	 * @return the created reader
	 * @throws PulsarException if any {@link PulsarClientException} occurs communicating
	 * with Pulsar
	 */
	Reader<T> createReader(@Nullable List<String> topics, @Nullable MessageId messageId, Schema<T> schema,
			@Nullable List<ReaderBuilderCustomizer<T>> customizers);

}
