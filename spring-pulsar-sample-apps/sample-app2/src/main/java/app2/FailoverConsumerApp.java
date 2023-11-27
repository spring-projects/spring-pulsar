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

package app2;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.PulsarClientException;
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
import org.springframework.pulsar.core.PulsarTopic;

@SpringBootApplication
public class FailoverConsumerApp {

	private static final Logger LOG = LoggerFactory.getLogger(FailoverConsumerApp.class);

	private static final String TOPIC = "failover-demo-topic";

	public static void main(String[] args) {
		SpringApplication.run(FailoverConsumerApp.class, args);
	}

	@Bean
	PulsarTopic failoverDemoTopic() {
		return PulsarTopic.builder(TOPIC).numberOfPartitions(3).build();
	}

	@Bean
	ApplicationRunner runner(PulsarTemplate<String> template) {
		return (args) -> {
			for (int i = 0; i < 10; i++) {
				sendMessage(0, template, new PartitionZeroRouter());
				sendMessage(1, template, new PartitionOneRouter());
				sendMessage(2, template, new PartitionTwoRouter());
			}
		};
	}

	private void sendMessage(int partition, PulsarTemplate<String> template, MessageRouter router) throws PulsarClientException {
		var msg = "hello_" + partition;
		template.newMessage(msg).withTopic(TOPIC)
				.withProducerCustomizer(builder -> builder.messageRouter(router)).sendAsync();
		LOG.info("++++++PRODUCE_{} {}------", partition, msg);
	}

	@PulsarListener(topics = TOPIC, subscriptionName = TOPIC+"-sub", subscriptionType = SubscriptionType.Failover)
	void listen0(String msg) {
		LOG.info("++++++CONSUME_0 {}------", msg);
	}

	@PulsarListener(topics = TOPIC, subscriptionName = TOPIC+"-sub", subscriptionType = SubscriptionType.Failover)
	void listen1(String msg) {
		LOG.info("++++++CONSUME_1 {}------", msg);
	}

	@PulsarListener(topics = TOPIC, subscriptionName = TOPIC+"-sub", subscriptionType = SubscriptionType.Failover)
	void listen2(String msg) {
		LOG.info("++++++CONSUME_2 {}------", msg);
	}

	static class PartitionZeroRouter implements MessageRouter {
		@Override
		public int choosePartition(Message<?> msg, TopicMetadata metadata) {
			return 0;
		}
	}

	static class PartitionOneRouter implements MessageRouter {
		@Override
		public int choosePartition(Message<?> msg, TopicMetadata metadata) {
			return 1;
		}
	}

	static class PartitionTwoRouter implements MessageRouter {
		@Override
		public int choosePartition(Message<?> msg, TopicMetadata metadata) {
			return 2;
		}
	}

}
