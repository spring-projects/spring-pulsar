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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.springframework.pulsar.test.support.ConsumedMessagesConditions.desiredMessageCount;

import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.core.PulsarTemplate;

/**
 * Tests for {@link PulsarConsumerTestUtil}.
 *
 * @author Jonas Geiregat
 */
class PulsarConsumerTestUtilTests implements PulsarTestContainerSupport {

	private PulsarTemplate<Object> pulsarTemplate;

	private DefaultPulsarConsumerFactory<String> pulsarConsumerFactory;

	private static String testTopic(String suffix) {
		return "ptctut-topic-" + suffix;
	}

	@BeforeEach
	void prepareForTest() throws PulsarClientException {
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		this.pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient, List.of());
		this.pulsarTemplate = new PulsarTemplate<>(new DefaultPulsarProducerFactory<>(pulsarClient));
	}

	@Test
	void whenConditionIsSpecifiedMessagesAreConsumedUntilConditionIsMet() {
		var topic = testTopic("a");
		IntStream.range(0, 5).forEach(i -> pulsarTemplate.send(topic, "message-" + i));
		var msgs = PulsarConsumerTestUtil.consumeMessages(pulsarConsumerFactory)
			.fromTopic(topic)
			.withSchema(Schema.STRING)
			.awaitAtMost(Duration.ofSeconds(5))
			.until(desiredMessageCount(3))
			.get();
		assertThat(msgs).hasSize(3);
	}

	@Test
	void whenChainedConditionAreSpecifiedMessagesAreConsumedUntilTheyAreMet() {
		var topic = testTopic("b");
		IntStream.range(0, 5).forEach(i -> pulsarTemplate.send(topic, "message-" + i));
		ConsumedMessagesCondition<String> condition1 = ConsumedMessagesConditions
				.desiredMessageCount(5);
		ConsumedMessagesCondition<String> condition2 = ConsumedMessagesConditions
				.atLeastOneMessageMatches("message-1");
		var msgs = PulsarConsumerTestUtil.consumeMessages(pulsarConsumerFactory)
				.fromTopic(topic)
				.withSchema(Schema.STRING)
				.awaitAtMost(Duration.ofSeconds(5))
				.until(condition1.and(condition2))
				.get();
		assertThat(msgs).hasSize(5);
	}

	@Test
	void whenConditionIsNotSpecifiedMessagesAreConsumedUntilAwaitDuration() {
		var topic = testTopic("b");
		IntStream.range(0, 5).forEach(i -> pulsarTemplate.send(topic, "message-" + i));
		var msgs = PulsarConsumerTestUtil.consumeMessages(pulsarConsumerFactory)
			.fromTopic(topic)
			.withSchema(Schema.STRING)
			.awaitAtMost(Duration.ofSeconds(5))
			.until(null)
			.get();
		assertThat(msgs).extracting(Message::getValue)
			.containsExactlyInAnyOrderElementsOf(IntStream.range(0, 5).mapToObj(i -> "message-" + i).toList());
	}

	@Test
	void exceptionIsThrownWhenConditionNotMetWithinAwaitDuration() {
		assertThatExceptionOfType(ConditionTimeoutException.class)
			.isThrownBy(() -> PulsarConsumerTestUtil.consumeMessages(pulsarConsumerFactory)
				.fromTopic(testTopic("c"))
				.withSchema(Schema.STRING)
				.awaitAtMost(Duration.ofSeconds(5))
				.until(ConsumedMessagesConditions.desiredMessageCount(3))
				.get())
			.withMessage("Condition was not met within 5 seconds");
	}

	@Test
	void exceptionIsThrownWhenMultipleUntilConditionsAreChainedTogether() {
		var topic = testTopic("b");
		IntStream.range(0, 1).forEach(i -> pulsarTemplate.send(topic, "message-" + i));
		assertThatExceptionOfType(IllegalStateException.class)
			.isThrownBy(() -> PulsarConsumerTestUtil.consumeMessages(pulsarConsumerFactory)
				.fromTopic(topic)
				.withSchema(Schema.STRING)
				.awaitAtMost(Duration.ofSeconds(5))
				.until(ConsumedMessagesConditions.desiredMessageCount(1))
				.until(ConsumedMessagesConditions.atLeastOneMessageMatches("message-0"))
				.get())
			.withMessage(
				"Multiple calls to 'until' are not allowed. Use 'and' to combine conditions."
			);
	}

	@Test
	void consumerFactoryCannotBeNull() {
		PulsarConsumerFactory<String> consumerFactory = null;
		assertThatIllegalArgumentException().isThrownBy(() -> PulsarConsumerTestUtil.consumeMessages(consumerFactory))
			.withMessage("PulsarConsumerFactory must not be null");
	}

	@Test
	void topicCannotBeNull() {
		assertThatIllegalArgumentException()
			.isThrownBy(() -> PulsarConsumerTestUtil.consumeMessages(pulsarConsumerFactory).fromTopic(null))
			.withMessage("Topic must not be null");
	}

	@Test
	void schemaCannotBeNull() {
		assertThatIllegalArgumentException()
			.isThrownBy(() -> PulsarConsumerTestUtil.consumeMessages(pulsarConsumerFactory)
				.fromTopic("topic-foo")
				.withSchema(null))
			.withMessage("Schema must not be null");
	}

	@Test
	void awaitAtMostCannotBeNull() {
		assertThatIllegalArgumentException()
			.isThrownBy(() -> PulsarConsumerTestUtil.consumeMessages(pulsarConsumerFactory)
				.fromTopic("topic-foo")
				.withSchema(Schema.STRING)
				.awaitAtMost(null))
			.withMessage("Timeout must not be null");
	}

}
