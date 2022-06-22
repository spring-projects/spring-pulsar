package org.springframework.pulsar.core;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.config.PulsarClientConfiguration;
import org.springframework.pulsar.config.PulsarClientFactoryBean;

public class SenderString {

	public static void main(String[] args) throws PulsarClientException {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(ConfigString.class);
		context.getBean(SenderString.class).send();
		System.exit(0);
	}

	private final PulsarTemplate<String> template;

	public SenderString(PulsarTemplate<String> template) {
		this.template = template;
	}

	public void send() throws PulsarClientException {
		final CompletableFuture<MessageId> future = this.template.sendAsync("hello john doe");
		future.thenAccept(m -> System.out.println("Got " + m));
		try {
			future.get();
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		catch (ExecutionException e) {
			e.printStackTrace();
		}
	}

}


@Configuration
class ConfigString {

	@Bean
	public PulsarProducerFactory<String> pulsarProducerFactory(PulsarClient pulsarClient) {
		Map<String, Object> config = new HashMap<>();
		config.put("topicName", "foo-1");
		return new DefaultPulsarProducerFactory<>(pulsarClient, config);
	}

	@Bean
	public PulsarClientFactoryBean pulsarClientFactoryBean(PulsarClientConfiguration pulsarClientConfiguration) {
		return new PulsarClientFactoryBean(pulsarClientConfiguration);
	}

	@Bean
	public PulsarClientConfiguration pulsarClientConfiguration() {
		return new PulsarClientConfiguration();
	}

	@Bean
	public PulsarTemplate<String> pulsarTemplate(PulsarProducerFactory<String> pulsarProducerFactory) {
		return new PulsarTemplate<>(pulsarProducerFactory);
	}

	@Bean
	public SenderString senderString(PulsarTemplate<String> template) {
		return new SenderString(template);
	}

}

