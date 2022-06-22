package experiments.basic;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class PulsarAppTry {

	public static void main(String[] args) {

		SpringApplication.run(PulsarAppTry.class, args);
	}

//	@Bean
//	public PulsarProducerFactory<String> pulsarProducerFactory(PulsarClient pulsarClient) {
//		Map<String, Object> config = new HashMap<>();
//		config.put("topicName", "foo-1");
//		return new DefaultPulsarProducerFactory<>(pulsarClient, config);
//	}
//
//	@Bean
//	public PulsarClientFactoryBean pulsarClientFactoryBean(PulsarClientConfiguration pulsarClientConfiguration) {
//		return new PulsarClientFactoryBean(pulsarClientConfiguration);
//	}
//
//	@Bean
//	public PulsarClientConfiguration pulsarClientConfiguration() {
//		return new PulsarClientConfiguration();
//	}

//	@Bean
//	public PulsarTemplate<String> pulsarTemplate(PulsarProducerFactory<String> pulsarProducerFactory) {
//		return new PulsarTemplate<>(pulsarProducerFactory);
//	}
//
//	@Bean
//	public PulsarConsumerFactory<?> pulsarConsumerFactory(PulsarClient pulsarClient) {
//
//		Map<String, Object> config = new HashMap<>();
////		final HashSet<String> strings = new HashSet<>();
////		strings.add("foobar-012");
////		config.put("topicNames", strings);
////		config.put("subscriptionName", "foobar-sb-012");
//
//		return new DefaultPulsarConsumerFactory<>(pulsarClient, config);
//	}
//
//	@Bean
//	PulsarListenerContainerFactory<?> pulsarListenerContainerFactory(PulsarConsumerFactory<Object> pulsarConsumerFactory) {
//		final PulsarListenerContainerFactoryImpl<?, ?> pulsarListenerContainerFactory = new PulsarListenerContainerFactoryImpl<>();
//		pulsarListenerContainerFactory.setPulsarConsumerFactory(pulsarConsumerFactory);
//		return pulsarListenerContainerFactory;
//	}

//	@PulsarListener(subscriptionName = "hello-pulsar-listener", topics = "foo-1")
//	public void listen(String foo) {
//		System.out.println("Message Received: " + foo);
//	}

//	@Configuration(proxyBeanMethods = false)
//	@EnablePulsar
//	static class EnablePulsarConfiguration {
//
//	}

}
