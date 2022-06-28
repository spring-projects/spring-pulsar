package apps.five;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.pulsar.annotation.PulsarListener;

@SpringBootApplication
public class FailoverConsumer {


	public static void main(String[] args) {
		String[] args1 = new String[]{
//				"--spring.pulsar.consumer.subscription-type=Failover",
				"--spring.pulsar.producer.messageRoutingMode=CustomPartition"};
		SpringApplication.run(FailoverConsumer.class, args1);
	}


	@PulsarListener(subscriptionName = "failover-subscription-demo",  topics = "failover-demo-topic", subscriptionType = "shared")
	public void listen1(String foo) {
		System.out.println("Message Received: " + foo);
	}


}

