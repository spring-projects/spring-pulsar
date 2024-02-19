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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.pulsar.test.support.Conditions.desiredMessageCount;

import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;

/**
 * Tests for {@link PulsarTestConsumer}.
 *
 * @author Jonas Geiregat
 */
class PulsarTestConsumerTest implements PulsarTestContainerSupport {

	private PulsarTemplate<Object> pulsarTemplate;

	private DefaultPulsarConsumerFactory<Object> pulsarConsumerFactory;

	@BeforeEach
	void setup() throws PulsarClientException {
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		this.pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient, List.of());
		this.pulsarTemplate = new PulsarTemplate<>(new DefaultPulsarProducerFactory<>(pulsarClient));
	}

	@Test
	void consumerReturnsWhenConditionIsMet() {
		var testConsumer = PulsarTestConsumer.consumeMessages(pulsarConsumerFactory)
			.fromTopic("topic-a")
			.withSchema(Schema.STRING);

		IntStream.range(0, 10).forEach(i -> pulsarTemplate.send("topic-a", "message-" + i));

		List<Message<String>> messages = testConsumer.until(desiredMessageCount(2));

		assertThat(messages).hasSize(2).map(Message::getValue).containsExactly("message-0", "message-1");
	}

	@Test
	void throwExceptionWhenConditionIsNotMet() {
		var testConsumer = PulsarTestConsumer.consumeMessages(pulsarConsumerFactory)
			.fromTopic("topic-b")
			.withSchema(Schema.STRING)
			.awaitAtMost(Duration.ofSeconds(5));

		ThrowingCallable consume = () -> testConsumer.until(desiredMessageCount(20));

		assertThatThrownBy(consume).isInstanceOf(PulsarTimeOutException.class);
	}

	@Test
	void throwExceptionWhenTimeoutReached() {
		var testConsumer = PulsarTestConsumer.consumeMessages(pulsarConsumerFactory)
			.fromTopic("topic-c")
			.withSchema(Schema.STRING)
			.awaitAtMost(Duration.ofSeconds(5));

		ThrowingCallable consume = () -> testConsumer.until(desiredMessageCount(2));

		assertThatThrownBy(consume).isInstanceOf(PulsarTimeOutException.class);
	}

}
