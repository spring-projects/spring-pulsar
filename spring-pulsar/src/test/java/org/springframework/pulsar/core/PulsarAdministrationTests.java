package org.springframework.pulsar.core;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;


@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class PulsarAdministrationTests extends AbstractContainerBaseTests {

	private static final String NAMESPACE = "public/default";

	@Autowired
	private ApplicationContext applicationContext;

	@Configuration(proxyBeanMethods = false)
	static class AdminConfiguration {
		@Bean
		PulsarAdministration admin() {
			return new PulsarAdministration(Map.of("serviceUrl", getHttpServiceUrl()));
		}
	}

	List<String> getTopics() throws Exception {
		try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(getHttpServiceUrl()).build()) {
			return admin.topics().getList(NAMESPACE);
		}
	}

	void assertTopicsExist(List<String> actualTopics, ObjectProvider<PulsarTopic> expected) {
		List<String> expectedTopics= expected.stream()
				.<String>mapMulti((topic, consumer) -> {
					if (topic.isPartitioned()) {
						for (int i = 0; i < topic.numberOfPartitions(); i++) {
							consumer.accept(topic.getFullyQualifiedTopicName() + "-partition-" + i);
						}
					} else {
						consumer.accept(topic.getFullyQualifiedTopicName());
					}

				}).toList();
		assertThat(actualTopics).containsAll(expectedTopics);
	}

	@Nested
	@ContextConfiguration(classes = CreateMissingTopicsTest.CreateMissingTopicsConfig.class)
	class CreateMissingTopicsTest {

		@Test
		void testTopics(@Autowired ObjectProvider<PulsarTopic> expectedTopics) throws Exception {
			List<String> actualTopics = getTopics();
			assertTopicsExist(actualTopics, expectedTopics);
		}

		@Configuration(proxyBeanMethods = false)
		static class CreateMissingTopicsConfig {

			@Bean
			PulsarTopic nonPartitionedTopic() {
				return PulsarTopic.builder("cmt-non-partitioned-1").build();
			}

			@Bean
			PulsarTopic nonPartitionedTopic2() {
				return PulsarTopic.builder("cmt-non-partitioned-2").build();
			}

			@Bean
			PulsarTopic partitionedTopic() {
				return PulsarTopic.builder("cmt-partitioned-1").setNumberOfPartitions(4).build();
			}
		}

	}

	@Nested
	@ContextConfiguration(classes = IncrementPartitionCountTest.IncrementPartitionCountConfig.class)
	class IncrementPartitionCountTest {

		@Test
		void testTopics(@Autowired ObjectProvider<PulsarTopic> expectedTopics) throws Exception {
			List<String> actualTopics = getTopics();
			assertTopicsExist(actualTopics, expectedTopics);
		}

		@Configuration(proxyBeanMethods = false)
		static class IncrementPartitionCountConfig {

			static {
				try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(getHttpServiceUrl()).build()) {
					admin.topics().createPartitionedTopic("ipc-partitioned-1", 2);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}

			@Bean
			PulsarTopic partitionedTopic() {
				return PulsarTopic.builder("ipc-partitioned-1").setNumberOfPartitions(4).build();
			}
		}
	}

}
