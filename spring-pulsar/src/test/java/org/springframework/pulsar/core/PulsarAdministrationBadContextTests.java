package org.springframework.pulsar.core;

import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import java.util.Map;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


public class PulsarAdministrationBadContextTests extends AbstractContainerBaseTests {

		@Test
		void testDecrementingPartitionCount() {
			assertThatIllegalStateException()
					.isThrownBy(() -> new AnnotationConfigApplicationContext(DecrementPartitionCountConfig.class).close())
					.withMessage("Topic persistent://public/default/dpc-partitioned-1 found with 8 partitions. Needs to be deleted first.");
		}

		@Configuration(proxyBeanMethods = false)
		static class DecrementPartitionCountConfig {
			static {
				try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(getHttpServiceUrl()).build()) {
					admin.topics().createPartitionedTopic("dpc-partitioned-1", 8);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}

			@Bean
			PulsarAdministration admin() {
				return new PulsarAdministration(Map.of("serviceUrl", getHttpServiceUrl()));
			}

			@Bean
			PulsarTopic partitionedTopic() {
				return PulsarTopic.builder("dpc-partitioned-1").numberOfPartitions(4).build();
			}
		}
	}
