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
import java.util.Random;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.TopicMetadata;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.pulsar.core.PulsarTemplate;

@SpringBootApplication
public class ProducerApp {


	public static void main(String[] args) {
		String[] args1 = new String[]{
//				"--spring.pulsar.consumer.subscription-type=Failover",
				"--spring.pulsar.producer.messageRoutingMode=CustomPartition"};
		SpringApplication.run(ProducerApp.class, args1);
	}

	@Bean
	public ApplicationRunner runner(PulsarTemplate<String> pulsarTemplate) {
		String topic = "failover-demo-topic";
		return args -> {
			for (int i = 0; i < 100; i++) {
				pulsarTemplate.sendAsync(topic, "hello john doex " + new Random().nextInt(), new FooRouter());
				pulsarTemplate.sendAsync(topic, "hello alice doex " + new Random().nextInt(), new BarRouter());
				if (i % 2 == 0) {
					pulsarTemplate.sendAsync(topic, "hello buzz doex " + new Random().nextInt(), new BuzzRouter());
				}
				Thread.sleep(5_000);
			}
		};
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
