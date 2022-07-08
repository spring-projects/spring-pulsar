/*
 * Copyright 2022 the original author or authors.
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

import java.util.Map;

import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

/**
 * Pulsar producer factory interface.
 *
 * @param <T> producer payload type.
 *
 * @author Soby Chacko
 */
public interface PulsarProducerFactory<T> {

	Producer<T> createProducer(Schema<T> schema) throws PulsarClientException;

	Producer<T> createProducer(Schema<T> schema, MessageRouter messageRouter) throws PulsarClientException;

	Map<String, Object> getProducerConfig();
}
