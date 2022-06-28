package experiments.basic.producer;

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
		pulsarTemplate.setDefaultTopicName("failover-demo-topic");
		return args -> {
			for (int i = 0; i < 100; i++) {
				System.out.println("current i: " + i);
				pulsarTemplate.sendAsync("hello john doex " + new Random().nextInt(), new FooRouter());
				pulsarTemplate.sendAsync("hello alice doex " + new Random().nextInt(), new BarRouter());
				if (i % 2 == 0) {
					pulsarTemplate.sendAsync("hello buzz doex " + new Random().nextInt(), new BuzzRouter());
				}
				Thread.sleep(5_000);
				System.out.println("------------------------");
			}
			System.exit(0);
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
