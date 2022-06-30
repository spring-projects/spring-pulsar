package apps.non.boot;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.config.PulsarClientConfiguration;
import org.springframework.pulsar.config.PulsarClientFactoryBean;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.PulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;

public class Sender {

	public static void main(String[] args) throws PulsarClientException {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Config.class);
		context.getBean(Sender.class).send();
		System.exit(0);
	}

	private final PulsarTemplate<byte[]> template;

	public Sender(PulsarTemplate<byte[]> template) {
		this.template = template;
	}

	public void send() throws PulsarClientException {
		this.template.send("foobar-fuzzy".getBytes(StandardCharsets.UTF_8));
	}

}


@Configuration
class Config {

	@Bean
	public PulsarProducerFactory<byte[]> pulsarProducerFactory(PulsarClient pulsarClient) {
		Map<String, Object> config = new HashMap<>();
		config.put("topicName", "foo-1");
		return new DefaultPulsarProducerFactory<>(pulsarClient, config);
	}

	@Bean
	public PulsarTemplate<byte[]> pulsarTemplate(PulsarProducerFactory<byte[]> pulsarProducerFactory) {
		return new PulsarTemplate<>(pulsarProducerFactory);
	}

	@Bean
	public Sender sender(PulsarTemplate<byte[]> template) {
		return new Sender(template);
	}

	@Bean
	public PulsarClientFactoryBean pulsarClientFactoryBean(PulsarClientConfiguration pulsarClientConfiguration) {
		return new PulsarClientFactoryBean(pulsarClientConfiguration);
	}

	@Bean
	public PulsarClientConfiguration pulsarClientConfiguration() {
		return new PulsarClientConfiguration();
	}

}

