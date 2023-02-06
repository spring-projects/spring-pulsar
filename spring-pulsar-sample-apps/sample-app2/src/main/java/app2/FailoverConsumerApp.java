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

package app2;

import java.io.Serial;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TopicMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.core.PulsarTemplate;

@SpringBootApplication
public class FailoverConsumerApp {

	private final Logger logger = LoggerFactory.getLogger(FailoverConsumerApp.class);

	public static void main(String[] args) {
		SpringApplication.run(FailoverConsumerApp.class, "--spring.pulsar.producer.messageRoutingMode=CustomPartition");
	}

	@Bean
	ApplicationRunner runner(PulsarTemplate<String> pulsarTemplate) {
		String topic = "failover-demo-topic";
		return args -> {
			for (int i = 0; i < 10; i++) {
				pulsarTemplate.newMessage("hello john doe 0 ").withTopic(topic)
						.withProducerCustomizer(builder -> builder.messageRouter(new FooRouter())).sendAsync();
				pulsarTemplate.newMessage("hello alice doe 1").withTopic(topic)
						.withProducerCustomizer(builder -> builder.messageRouter(new BarRouter())).sendAsync();
				pulsarTemplate.newMessage("hello buzz doe 2").withTopic(topic)
						.withProducerCustomizer(builder -> builder.messageRouter(new BuzzRouter())).sendAsync();
				Thread.sleep(1_000);
			}
		};
	}

	@PulsarListener(subscriptionName = "failover-subscription-demo", topics = "failover-demo-topic",
			subscriptionType = SubscriptionType.Failover)
	void listen1(String foo) {
		this.logger.info("failover-listen1 : " + foo);
	}

	@PulsarListener(subscriptionName = "failover-subscription-demo", topics = "failover-demo-topic",
			subscriptionType = SubscriptionType.Failover)
	void listen2(String foo) {
		this.logger.info("failover-listen2 : " + foo);
	}

	@PulsarListener(subscriptionName = "failover-subscription-demo", topics = "failover-demo-topic",
			subscriptionType = SubscriptionType.Failover)
	void listen(String foo) {
		this.logger.info("failover-listen3 : " + foo);
	}

	static class FooRouter implements MessageRouter {

		@Serial
		private static final long serialVersionUID = -1L;

		@Override
		public int choosePartition(Message<?> msg, TopicMetadata metadata) {
			return 0;
		}

	}

	static class BarRouter implements MessageRouter {

		@Serial
		private static final long serialVersionUID = -1L;

		@Override
		public int choosePartition(Message<?> msg, TopicMetadata metadata) {
			return 1;
		}

	}

	static class BuzzRouter implements MessageRouter {

		@Serial
		private static final long serialVersionUID = -1L;

		@Override
		public int choosePartition(Message<?> msg, TopicMetadata metadata) {
			return 2;
		}

	}

}
