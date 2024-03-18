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
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import org.springframework.pulsar.PulsarException;
import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.core.PulsarTemplate;

/**
 * Tests for {@link PulsarConsumerTestUtil}.
 *
 * @author Jonas Geiregat
 * @author Kartik Shrivastava
 * @author Chris Bono
 */
class PulsarConsumerTestUtilTests implements PulsarTestContainerSupport {

	private PulsarTemplate<Object> pulsarTemplate;

	private DefaultPulsarConsumerFactory<String> pulsarConsumerFactory;

	private PulsarClient pulsarClient;

	private static String testTopic(String suffix) {
		return "ptctut-topic-" + suffix;
	}

	@BeforeEach
	void prepareForTest() throws PulsarClientException {
		this.pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		this.pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient, List.of());
		this.pulsarTemplate = new PulsarTemplate<>(new DefaultPulsarProducerFactory<>(pulsarClient));
	}

	@AfterEach
	void cleanupFromTest() throws PulsarClientException {
		if (this.pulsarClient != null) {
			this.pulsarClient.close();
		}
	}

	@Test
	void whenConditionIsSpecifiedThenMessagesConsumedUntilConditionMet() {
		var topic = testTopic("cond");
		IntStream.range(0, 5).forEach(i -> pulsarTemplate.send(topic, "message-" + i));
		var consumerTestUtil = PulsarConsumerTestUtil.consumeMessages(pulsarConsumerFactory)
			.fromTopic(topic)
			.withSchema(Schema.STRING)
			.awaitAtMost(Duration.ofSeconds(5))
			.until(desiredMessageCount(3));
		assertThat(consumerTestUtil.get()).hasSize(3);
		assertThatLocallyCreatedClientIsNull(consumerTestUtil);
	}

	@Test
	void whenConditionIsNotSpecifiedThenMessagesAreConsumedUntilAwaitDuration() {
		var topic = testTopic("no-cond");
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
	void whenChainedConditionsAreSpecifiedThenMessagesConsumedUntilAllConditionsMet() {
		var topic = testTopic("chained-cond");
		IntStream.range(0, 5).forEach(i -> pulsarTemplate.send(topic, "message-" + i));
		ConsumedMessagesCondition<String> condition1 = ConsumedMessagesConditions.desiredMessageCount(5);
		ConsumedMessagesCondition<String> condition2 = ConsumedMessagesConditions.atLeastOneMessageMatches("message-1");
		var msgs = PulsarConsumerTestUtil.consumeMessages(pulsarConsumerFactory)
			.fromTopic(topic)
			.withSchema(Schema.STRING)
			.awaitAtMost(Duration.ofSeconds(5))
			.until(condition1.and(condition2))
			.get();
		assertThat(msgs).hasSize(5);
	}

	@Test
	void whenConditionNotMetWithinAwaitDurationThenExceptionIsThrown() {
		assertThatExceptionOfType(ConditionTimeoutException.class)
			.isThrownBy(() -> PulsarConsumerTestUtil.consumeMessages(pulsarConsumerFactory)
				.fromTopic(testTopic("cond-not-met"))
				.withSchema(Schema.STRING)
				.awaitAtMost(Duration.ofSeconds(5))
				.until(ConsumedMessagesConditions.desiredMessageCount(3))
				.get())
			.withMessage("Condition was not met within 5 seconds");
	}

	@Test
	void consumeMessagesWithNoArgsUsesPulsarContainerIfAvailable() {
		var topic = testTopic("no-arg");
		IntStream.range(0, 2).forEach(i -> pulsarTemplate.send(topic, "message-" + i));
		var consumerTestUtil = PulsarConsumerTestUtil.<String>consumeMessages()
			.fromTopic(topic)
			.withSchema(Schema.STRING)
			.awaitAtMost(Duration.ofSeconds(5))
			.until(desiredMessageCount(2));
		assertThat(consumerTestUtil.get()).hasSize(2);
		assertThatLocallyCreatedClientIsClosed(consumerTestUtil);
	}

	@Test
	void consumeMessagesWithNoArgsUsesDefaultUrlWhenPulsarContainerNotAvailable() {
		// @formatter::off
		try (MockedStatic<PulsarTestContainerSupport> containerSupport = Mockito
			.mockStatic(PulsarTestContainerSupport.class)) {
			containerSupport.when(PulsarTestContainerSupport::isContainerStarted).thenReturn(false);
			var topic = testTopic("no-arg-dft-url");
			assertThatExceptionOfType(PulsarException.class)
				.isThrownBy(() -> PulsarConsumerTestUtil.<String>consumeMessages()
					.fromTopic(topic)
					.withSchema(Schema.STRING)
					.awaitAtMost(Duration.ofSeconds(2))
					.get())
				.withStackTraceContaining("Connection refused: localhost");
		}
		// @formatter:on
	}

	@Test
	void consumeMessagesWithBrokerUrl() {
		var topic = testTopic("url-arg");
		IntStream.range(0, 2).forEach(i -> pulsarTemplate.send(topic, "message-" + i));
		var consumerTestUtil = PulsarConsumerTestUtil
			.<String>consumeMessages(PulsarTestContainerSupport.getPulsarBrokerUrl())
			.fromTopic(topic)
			.withSchema(Schema.STRING)
			.awaitAtMost(Duration.ofSeconds(5))
			.until(desiredMessageCount(2));
		assertThat(consumerTestUtil.get()).hasSize(2);
		assertThatLocallyCreatedClientIsClosed(consumerTestUtil);
	}

	@Test
	void consumeMessagesWithPulsarClient() {
		var topic = testTopic("client-arg");
		IntStream.range(0, 2).forEach(i -> pulsarTemplate.send(topic, "message-" + i));
		var consumerTestUtil = PulsarConsumerTestUtil.<String>consumeMessages(this.pulsarClient)
			.fromTopic(topic)
			.withSchema(Schema.STRING)
			.awaitAtMost(Duration.ofSeconds(5))
			.until(desiredMessageCount(2));
		assertThat(consumerTestUtil.get()).hasSize(2);
		assertThatLocallyCreatedClientIsNull(consumerTestUtil);
	}

	private void assertThatLocallyCreatedClientIsNull(ConditionsSpec<?> consumerTestUtil) {
		assertThat(consumerTestUtil).extracting("locallyCreatedPulsarClient").isNull();
	}

	private void assertThatLocallyCreatedClientIsClosed(ConditionsSpec<?> consumerTestUtil) {
		assertThat(consumerTestUtil).extracting("locallyCreatedPulsarClient")
			.isNotNull()
			.asInstanceOf(InstanceOfAssertFactories.type(PulsarClient.class))
			.extracting(PulsarClient::isClosed)
			.isEqualTo(Boolean.TRUE);
	}

	@Test
	void untilCannotBeCalledMultipleTimes() {
		var topic = testTopic("until-multi");
		assertThatExceptionOfType(IllegalStateException.class)
			.isThrownBy(() -> PulsarConsumerTestUtil.consumeMessages(pulsarConsumerFactory)
				.fromTopic(topic)
				.withSchema(Schema.STRING)
				.awaitAtMost(Duration.ofSeconds(5))
				.until(ConsumedMessagesConditions.desiredMessageCount(1))
				.until(ConsumedMessagesConditions.atLeastOneMessageMatches("message-0"))
				.get())
			.withMessage("Multiple calls to 'until' are not allowed. Use 'and' to combine conditions.");
	}

	@Test
	void brokerUrlCannotBeNull() {
		String url = null;
		assertThatIllegalArgumentException().isThrownBy(() -> PulsarConsumerTestUtil.consumeMessages(url))
			.withMessage("url must not be null");
	}

	@Test
	void pulsarClientCannotBeNull() {
		PulsarClient localPulsarClient = null;
		assertThatIllegalArgumentException().isThrownBy(() -> PulsarConsumerTestUtil.consumeMessages(localPulsarClient))
			.withMessage("pulsarClient must not be null");
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
