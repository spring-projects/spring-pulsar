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

package org.springframework.pulsar.reactive.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.adapter.AdaptedReactivePulsarClientFactory;
import org.apache.pulsar.reactive.client.api.MutableReactiveMessageReaderSpec;
import org.apache.pulsar.reactive.client.api.ReactiveMessageReader;
import org.apache.pulsar.reactive.client.api.ReactiveMessageReaderSpec;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DefaultReactivePulsarReaderFactory}.
 *
 * @author Christophe Bornet
 */
class DefaultReactiveMessageReaderFactoryTests {

	private static final Schema<String> schema = Schema.STRING;

	@Test
	void createReader() {
		MutableReactiveMessageReaderSpec spec = new MutableReactiveMessageReaderSpec();
		spec.setReaderName("test-reader");
		DefaultReactivePulsarReaderFactory<String> readerFactory = new DefaultReactivePulsarReaderFactory<>(
				AdaptedReactivePulsarClientFactory.create((PulsarClient) null), spec);

		ReactiveMessageReader<String> reader = readerFactory.createReader(schema);

		assertThat(reader).extracting("readerSpec", InstanceOfAssertFactories.type(ReactiveMessageReaderSpec.class))
				.extracting(ReactiveMessageReaderSpec::getReaderName).isEqualTo("test-reader");
	}

	@Test
	void createReaderWithCustomizer() {
		MutableReactiveMessageReaderSpec spec = new MutableReactiveMessageReaderSpec();
		spec.setReaderName("test-reader");
		DefaultReactivePulsarReaderFactory<String> readerFactory = new DefaultReactivePulsarReaderFactory<>(
				AdaptedReactivePulsarClientFactory.create((PulsarClient) null), spec);

		ReactiveMessageReader<String> reader = readerFactory.createReader(schema,
				Collections.singletonList(builder -> builder.readerName("new-test-reader")));

		assertThat(reader).extracting("readerSpec", InstanceOfAssertFactories.type(ReactiveMessageReaderSpec.class))
				.extracting(ReactiveMessageReaderSpec::getReaderName).isEqualTo("new-test-reader");
	}

}
