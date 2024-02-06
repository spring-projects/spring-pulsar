package org.springframework.pulsar.test.support;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;

class PulsarTestConsumerTest implements PulsarTestContainerSupport {

	private PulsarTestConsumer pulsarTestConsumer;

	private PulsarTemplate<User> pulsarTemplate;

	@BeforeEach
	void pulsarTestConsumer() throws PulsarClientException {
		var pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
		var pulsarConsumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient, List.of());
		this.pulsarTestConsumer = new PulsarTestConsumer(pulsarConsumerFactory, Duration.ofSeconds(5));
		this.pulsarTemplate = new PulsarTemplate<>(new DefaultPulsarProducerFactory<>(pulsarClient));
	}

	@Nested
	class ReceiveEarliestMessages {

		@Test
		void timeoutWithNoMessage() {
			List<Message<User>> messages = pulsarTestConsumer.receiveEarliestMessages(1,
					List.of("persistent://public/default/topic-1"), Schema.JSON(User.class));

			assertThat(messages).isEmpty();
		}

		@Test
		void timeoutWithLessThanExpectedMessages() {
			pulsarTemplate.send("persistent://public/default/topic-2", new User("John"), Schema.JSON(User.class));

			List<Message<User>> messages = pulsarTestConsumer.receiveEarliestMessages(5,
					List.of("persistent://public/default/topic-2"), Schema.JSON(User.class));

			assertThat(messages).hasSize(1)
					.extracting(Message::getValue)
					.containsExactly(new User("John"));
		}

		@Test
		void onlyReceiveDesiredNumberOfMessages() {
			IntStream.range(0, 10)
				.forEach(i -> pulsarTemplate.send("persistent://public/default/topic-3", new User("John-" + i),
						Schema.JSON(User.class)));

			List<Message<User>> messages = pulsarTestConsumer.receiveEarliestMessages(5,
					List.of("persistent://public/default/topic-3"), Schema.JSON(User.class));

			assertThat(messages).hasSize(5)
					.extracting(Message::getValue)
					.containsExactly(new User("John-0"), new User("John-1"), new User("John-2"), new User("John-3"),
							new User("John-4"));
		}

	}

	@Nested
	class ReceiveLatestMessages {

		@Test
		void timeoutWithNoMessage() {
			List<Message<User>> messages = pulsarTestConsumer.receiveLatestMessages(1,
					List.of("persistent://public/default/topic-4"), Schema.JSON(User.class));

			assertThat(messages).isEmpty();
		}

		@Test
		void timeoutWithLessThanExpectedMessages() {
			IntStream.range(0, 10)
					.forEach(i -> pulsarTemplate.send("persistent://public/default/topic-5", new User("John-" + i),
							Schema.JSON(User.class)));
			List<Message<User>> messages = pulsarTestConsumer.receiveEarliestMessages(5,
					List.of("persistent://public/default/topic-5"), Schema.JSON(User.class));
			assertThat(messages).hasSize(5);

			messages = pulsarTestConsumer.receiveLatestMessages(15, List.of("persistent://public/default/topic-5"),
					Schema.JSON(User.class));
			assertThat(messages).hasSize(5)
					.extracting(Message::getValue)
					.containsExactly(new User("John-5"), new User("John-6"), new User("John-7"), new User("John-8"),
							new User("John-9"));
		}

		@Test
		void onlyReceiveDesiredNumberOfMessages() throws InterruptedException {
			IntStream.range(0, 10)
				.forEach(i -> pulsarTemplate.send("persistent://public/default/topic-6", new User("John-" + i),
						Schema.JSON(User.class)));
			List<Message<User>> messages = pulsarTestConsumer.receiveEarliestMessages(5,
					List.of("persistent://public/default/topic-6"), Schema.JSON(User.class));
			assertThat(messages).hasSize(5);

			messages = pulsarTestConsumer.receiveLatestMessages(2, List.of("persistent://public/default/topic-6"),
					Schema.JSON(User.class));

			assertThat(messages)
					.extracting(Message::getValue)
					.containsExactly(new User("John-5"), new User("John-6"));
		}

	}

}
