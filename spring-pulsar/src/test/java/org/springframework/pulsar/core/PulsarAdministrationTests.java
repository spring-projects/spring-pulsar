/*
 * Copyright 2022-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.pulsar.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationBasic;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Tests for {@link PulsarAdministration}.
 *
 * @author Alexander Preuß
 * @author Chris Bono
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class PulsarAdministrationTests implements PulsarTestContainerSupport {

	private static final String NAMESPACE = "public/default";

	@Autowired
	private PulsarAdmin pulsarAdminClient;

	@Autowired
	private PulsarAdministration pulsarAdministration;

    private void assertThatTopicsExist(List<PulsarTopic> expected) throws PulsarAdminException {
        assertThatTopicsExistIn(expected, NAMESPACE);
    }

    private void assertThatTopicsExistIn(List<PulsarTopic> expected, String namespace) throws PulsarAdminException {
        List<String> expectedTopics = expectedTopics(expected);
        assertThat(pulsarAdminClient.topics().getList(namespace)).containsAll(expectedTopics);
    }

    static List<String> expectedTopics(List<PulsarTopic> expected) {
        return expected.stream().<String>mapMulti((topic, consumer) -> {
            if (topic.isPartitioned()) {
                for (int i = 0; i < topic.numberOfPartitions(); i++) {
                    consumer.accept(topic.getFullyQualifiedTopicName() + "-partition-" + i);
                }
            } else {
                consumer.accept(topic.getFullyQualifiedTopicName());
            }

        }).toList();
    }

	@Test
	void constructorRespectsAuthenticationProps() {
		Map<String, Object> props = new HashMap<>();
		props.put("authPluginClassName", "org.apache.pulsar.client.impl.auth.AuthenticationBasic");
		props.put("authParams", "{\"userId\":\"foo\", \"password\":\"bar\"}");
		PulsarAdministration admin = new PulsarAdministration(props);

		assertThat(admin).extracting("adminBuilder").asInstanceOf(type(PulsarAdminBuilder.class)).extracting("conf")
				.asInstanceOf(type(ClientConfigurationData.class))
				.extracting(ClientConfigurationData::getAuthentication).isInstanceOf(AuthenticationBasic.class)
				.hasFieldOrPropertyWithValue("userId", "foo").hasFieldOrPropertyWithValue("password", "bar");
	}

	@Configuration(proxyBeanMethods = false)
	static class AdminConfiguration {

		@Bean
		PulsarAdmin pulsarAdminClient() throws PulsarClientException {
			return PulsarAdmin.builder().serviceHttpUrl(PulsarTestContainerSupport.getHttpServiceUrl()).build();
		}

		@Bean
		PulsarAdministration pulsarAdministration() {
			return new PulsarAdministration(
					PulsarAdmin.builder().serviceHttpUrl(PulsarTestContainerSupport.getHttpServiceUrl()));
		}

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
    @ContextConfiguration(classes = CreateMissingTopicsInSeparateNamespacesTest.CreateMissingTopicsConfig.class)
    class CreateMissingTopicsInSeparateNamespacesTest {

        @Test
        void topicsExist(@Autowired ObjectProvider<PulsarTopic> expectedTopics) throws Exception {
            assertThatTopicsExistIn(expectedTopics.stream().filter(topic -> topic.topicName().contains(CreateMissingTopicsConfig.PUBLIC_BLUE_NAMESPACE)).toList(), CreateMissingTopicsConfig.PUBLIC_BLUE_NAMESPACE);
            assertThatTopicsExistIn(expectedTopics.stream().filter(topic -> topic.topicName().contains(CreateMissingTopicsConfig.PUBLIC_GREEN_NAMESPACE)).toList(), CreateMissingTopicsConfig.PUBLIC_GREEN_NAMESPACE);
        }


        @Configuration(proxyBeanMethods = false)
        static class CreateMissingTopicsConfig {

            public static final String PUBLIC_GREEN_NAMESPACE = "public/green";

            public static final String PUBLIC_BLUE_NAMESPACE = "public/blue";

            static {
                var pulsarAdminBuilder = PulsarAdmin.builder().serviceHttpUrl(PulsarTestContainerSupport.getHttpServiceUrl());
                try (var pulsarAdmin = pulsarAdminBuilder.build()) {
                    Set.of(PUBLIC_GREEN_NAMESPACE, PUBLIC_BLUE_NAMESPACE)
                            .forEach(ns -> {
                                try {
                                    pulsarAdmin.namespaces().createNamespace(ns);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Bean
            PulsarTopic partitionedGreenTopic() {
                return PulsarTopic.builder("persistent://%s/partitioned-1".formatted(PUBLIC_GREEN_NAMESPACE))
                        .numberOfPartitions(2).build();
            }

            @Bean
            PulsarTopic partitionedBlueTopic() {
                return PulsarTopic.builder("persistent://%s/partitioned-1".formatted(PUBLIC_BLUE_NAMESPACE))
                        .numberOfPartitions(2).build();
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
			assertThatIllegalStateException().isThrownBy(() -> pulsarAdministration.createOrModifyTopics(smallerTopic))
					.withMessage(
							"Topic persistent://public/default/dpc-partitioned-1 found with 8 partitions. Needs to be deleted first.");

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
