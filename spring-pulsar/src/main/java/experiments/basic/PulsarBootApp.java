package experiments.basic;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.core.PulsarTemplate;

@SpringBootApplication
public class PulsarBootApp {

	public static void main(String[] args) {
		SpringApplication.run(PulsarBootApp.class, args);
	}

	@Bean
	public ApplicationRunner runner(PulsarTemplate<String> pulsarTemplate) {
		pulsarTemplate.setDefaultTopicName("hello-pulsar-exclusive");
		return args -> {
//			for (int i = 0; i < 100; i ++) {
//				pulsarTemplate.send("This is message " + (i + 1));
//			}

			pulsarTemplate.send("This is message ");

		};
	}

	@PulsarListener(subscriptionName = "test-exclusive-sub", topics = "hello-pulsar-exclusive")
	public void listen(String foo) {
		System.out.println("Message Received: " + foo);
	}


}
