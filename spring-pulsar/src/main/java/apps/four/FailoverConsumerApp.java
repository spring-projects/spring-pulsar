package apps.four;

import java.io.Serial;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.TopicMetadata;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.core.PulsarTemplate;

@SpringBootApplication
public class FailoverConsumerApp {


	public static void main(String[] args) {
		String[] args1 = new String[]{
//				"--spring.pulsar.consumer.subscription-type=Failover",
				"--spring.pulsar.producer.messageRoutingMode=CustomPartition"};
		SpringApplication.run(FailoverConsumerApp.class, args1);
	}

	@Bean
	public ApplicationRunner runner(PulsarTemplate<String> pulsarTemplate) {
		pulsarTemplate.setDefaultTopicName("failover-demo-topic");
		return args -> {
			for (int i = 0; i < 10; i++) {
				pulsarTemplate.sendAsync("hello john doe 0 ", new FooRouter());
				pulsarTemplate.sendAsync("hello alice doe 1", new BarRouter());
				pulsarTemplate.sendAsync("hello buzz doe 2", new BuzzRouter());
				Thread.sleep(1_000);
				System.out.println("------------------------");
			}
			System.exit(0);
		};
	}

	@PulsarListener(subscriptionName = "failover-subscription-demo",  topics = "failover-demo-topic", subscriptionType = "failover")
	public void listen1(String foo) {
		System.out.println("Message Received 1: " + foo);
	}

	@PulsarListener(subscriptionName = "failover-subscription-demo", topics = "failover-demo-topic", subscriptionType = "failover")
	public void listen2(String foo) {
		System.out.println("Message Received 2: " + foo);
	}

	@PulsarListener(subscriptionName = "failover-subscription-demo",  topics = "failover-demo-topic", subscriptionType = "failover")
	public void listen(String foo) {
		System.out.println("Message Received 3: " + foo);
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
