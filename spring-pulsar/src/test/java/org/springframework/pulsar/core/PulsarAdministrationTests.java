package org.springframework.pulsar.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.Collections;
import java.util.List;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;


@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class PulsarAdministrationTests extends AbstractContainerBaseTests {

	private static final String NAMESPACE = "public/default";

	@Autowired
	private PulsarAdmin pulsarAdminClient;

	@Autowired
	private PulsarAdministration pulsarAdministration;

	@Configuration(proxyBeanMethods = false)
	static class AdminConfiguration {
		@Bean
		PulsarAdmin pulsarAdminClient() throws PulsarClientException {
			return PulsarAdmin.builder().serviceHttpUrl(getHttpServiceUrl()).build();
		}
		@Bean
		PulsarAdministration pulsarAdministration() {
			return new PulsarAdministration(PulsarAdmin.builder().serviceHttpUrl(getHttpServiceUrl()));
		}
	}

	private void assertThatTopicsExist(List<PulsarTopic> expected) throws PulsarAdminException {
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
		assertThat(pulsarAdminClient.topics().getList(NAMESPACE)).containsAll(expectedTopics);
	}

	@Nested
	@ContextConfiguration(classes = CreateMissingTopicsTest.CreateMissingTopicsConfig.class)
	class CreateMissingTopicsTest {

		@Test
		void topicsExist(@Autowired ObjectProvider<PulsarTopic> expectedTopics) throws Exception {
			assertThatTopicsExist(expectedTopics.stream().toList());
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
				return PulsarTopic.builder("cmt-partitioned-1").numberOfPartitions(4).build();
			}
		}

	}

	@Nested
	@ContextConfiguration(classes = IncrementPartitionCountTest.IncrementPartitionCountConfig.class)
	class IncrementPartitionCountTest {

		@Test
		void topicsExist(@Autowired ObjectProvider<PulsarTopic> expectedTopics) throws Exception {
			assertThatTopicsExist(expectedTopics.stream().toList());
			PulsarTopic biggerTopic = PulsarTopic.builder("ipc-partitioned-1").numberOfPartitions(4).build();
			pulsarAdministration.createOrModifyTopics(biggerTopic);
			assertThatTopicsExist(Collections.singletonList(biggerTopic));
		}

		@Configuration(proxyBeanMethods = false)
		static class IncrementPartitionCountConfig {
			@Bean
			PulsarTopic smallerTopic() {
				return PulsarTopic.builder("ipc-partitioned-1").numberOfPartitions(1).build();
			}
		}
	}

	@Nested
	@ContextConfiguration(classes = DecrementPartitionCountTest.DecrementPartitionCountConfig.class)
	class DecrementPartitionCountTest {

		@Test
		void topicModificationThrows(@Autowired ObjectProvider<PulsarTopic> expectedTopics) throws Exception {
			assertThatTopicsExist(expectedTopics.stream().toList());
			PulsarTopic smallerTopic = PulsarTopic.builder("dpc-partitioned-1").numberOfPartitions(4).build();
			assertThatIllegalStateException()
					.isThrownBy(() -> pulsarAdministration.createOrModifyTopics(smallerTopic))
					.withMessage("Topic persistent://public/default/dpc-partitioned-1 found with 8 partitions. Needs to be deleted first.");

		}

		@Configuration(proxyBeanMethods = false)
		static class DecrementPartitionCountConfig {
			@Bean
			PulsarTopic biggerTopic() {
				return PulsarTopic.builder("dpc-partitioned-1").numberOfPartitions(8).build();
			}
		}
	}
}
