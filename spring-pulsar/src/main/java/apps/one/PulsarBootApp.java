package apps.one;

import org.apache.pulsar.common.schema.SchemaType;

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
	public ApplicationRunner runner(PulsarTemplate<Integer> pulsarTemplate) {
		pulsarTemplate.setDefaultTopicName("hello-pulsar-exclusive-1");
		return args -> {
//			for (int i = 0; i < 100; i ++) {
//				pulsarTemplate.send("This is message " + (i + 1));
//			}

			pulsarTemplate.send(250);

		};
	}

	@PulsarListener(subscriptionName = "test-exclusive-sub", topics = "hello-pulsar-exclusive-1", schemaType = SchemaType.AVRO)
	public void listen(int foo) {
		System.out.println("Message Received: " + foo);
	}


}
