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
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Tests for {@link PulsarAdministration}.
 *
 * @author Alexander Preuß
 * @author Chris Bono
 * @author Kirill Merkushev
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
@SuppressWarnings("JUnitMalformedDeclaration")
public class PulsarAdministrationTests implements PulsarTestContainerSupport {

	private static final String NAMESPACE = "public/default";

	@Autowired
	private PulsarAdmin pulsarAdminClient;

	@Autowired
	private PulsarAdministration pulsarAdministration;

	private void assertThatTopicsExist(List<PulsarTopic> expected) throws PulsarAdminException {
		assertThatTopicsExistIn(expected, NAMESPACE);
	}

	private void assertThatTopicsExistIn(List<PulsarTopic> expectedTopics, String namespace)
			throws PulsarAdminException {
		List<String> expectedFullyQualifiedTopicNames = expectedTopics.stream().<String>mapMulti((topic, consumer) -> {
			if (topic.isPartitioned()) {
				for (int i = 0; i < topic.numberOfPartitions(); i++) {
					consumer.accept(topic.getFullyQualifiedTopicName() + "-partition-" + i);
				}
			}
			else {
				consumer.accept(topic.getFullyQualifiedTopicName());
			}

		}).toList();
		assertThat(pulsarAdminClient.topics().getList(namespace)).containsAll(expectedFullyQualifiedTopicNames);
	}

	@Configuration(proxyBeanMethods = false)
	static class AdminConfiguration {

		@Bean
		PulsarAdmin pulsarAdminClient() throws PulsarClientException {
			return PulsarAdmin.builder().serviceHttpUrl(PulsarTestContainerSupport.getHttpServiceUrl()).build();
		}

		@Bean
		PulsarAdministration pulsarAdministration() {
			return new PulsarAdministration(PulsarTestContainerSupport.getHttpServiceUrl());
		}

	}

	@Nested
	@ContextConfiguration(classes = CreateMissingTopicsTests.CreateMissingTopicsConfig.class)
	class CreateMissingTopicsTests {

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
	@ContextConfiguration(classes = CreateMissingTopicsInSeparateNamespacesTests.CreateMissingTopicsConfig.class)
	class CreateMissingTopicsInSeparateNamespacesTests {

		@Test
		void topicsExist(@Autowired PulsarTopic partitionedGreenTopic, @Autowired PulsarTopic partitionedBlueTopic)
				throws PulsarAdminException {
			assertThatTopicsExistIn(Collections.singletonList(partitionedGreenTopic),
					CreateMissingTopicsConfig.PUBLIC_GREEN_NAMESPACE);
			assertThatTopicsExistIn(Collections.singletonList(partitionedBlueTopic),
					CreateMissingTopicsConfig.PUBLIC_BLUE_NAMESPACE);
		}

		@Configuration(proxyBeanMethods = false)
		static class CreateMissingTopicsConfig {

			public static final String PUBLIC_GREEN_NAMESPACE = "public/green";

			public static final String PUBLIC_BLUE_NAMESPACE = "public/blue";

			static {
				try (var pulsarAdmin = PulsarAdmin.builder()
					.serviceHttpUrl(PulsarTestContainerSupport.getHttpServiceUrl())
					.build()) {
					pulsarAdmin.namespaces().createNamespace(PUBLIC_GREEN_NAMESPACE);
					pulsarAdmin.namespaces().createNamespace(PUBLIC_BLUE_NAMESPACE);
				}
				catch (PulsarClientException | PulsarAdminException e) {
					throw new RuntimeException(e);
				}
			}

			@Bean
			PulsarTopic partitionedGreenTopic() {
				return PulsarTopic.builder("persistent://%s/partitioned-1".formatted(PUBLIC_GREEN_NAMESPACE))
					.numberOfPartitions(2)
					.build();
			}

			@Bean
			PulsarTopic partitionedBlueTopic() {
				return PulsarTopic.builder("persistent://%s/partitioned-1".formatted(PUBLIC_BLUE_NAMESPACE))
					.numberOfPartitions(2)
					.build();
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = IncrementPartitionCountTests.IncrementPartitionCountConfig.class)
	class IncrementPartitionCountTests {

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
	@ContextConfiguration(classes = DecrementPartitionCountTests.DecrementPartitionCountConfig.class)
	class DecrementPartitionCountTests {

		@Test
		void topicModificationThrows(@Autowired ObjectProvider<PulsarTopic> expectedTopics) throws Exception {
			assertThatTopicsExist(expectedTopics.stream().toList());
			PulsarTopic smallerTopic = PulsarTopic.builder("dpc-partitioned-1").numberOfPartitions(4).build();
			assertThatIllegalStateException().isThrownBy(() -> pulsarAdministration.createOrModifyTopics(smallerTopic))
				.withMessage(
						"Topic 'persistent://public/default/dpc-partitioned-1' found w/ 8 partitions but can't shrink to 4 - needs to be deleted first");

		}

		@Configuration(proxyBeanMethods = false)
		static class DecrementPartitionCountConfig {

			@Bean
			PulsarTopic biggerTopic() {
				return PulsarTopic.builder("dpc-partitioned-1").numberOfPartitions(8).build();
			}

		}

	}

	@Nested
	@ContextConfiguration
	class ConflictingTopicsTests {

		@Test
		void unpartitionedTopicAlreadyExists() {
			var unpartitionedTopic = PulsarTopic.builder("ctt-foo").numberOfPartitions(0).build();
			var partitionedTopic = PulsarTopic.builder("ctt-foo").numberOfPartitions(3).build();
			pulsarAdministration.createOrModifyTopics(unpartitionedTopic);
			assertThatIllegalStateException()
				.isThrownBy(() -> pulsarAdministration.createOrModifyTopics(partitionedTopic))
				.withMessage(
						"Topic 'persistent://public/default/ctt-foo' already exists un-partitioned - needs to be deleted first");
		}

		@Test
		void partitionedTopicAlreadyExists() {
			var unpartitionedTopic = PulsarTopic.builder("ctt-bar").numberOfPartitions(0).build();
			var partitionedTopic = PulsarTopic.builder("ctt-bar").numberOfPartitions(3).build();
			pulsarAdministration.createOrModifyTopics(partitionedTopic);
			assertThatIllegalStateException()
				.isThrownBy(() -> pulsarAdministration.createOrModifyTopics(unpartitionedTopic))
				.withMessage(
						"Topic 'persistent://public/default/ctt-bar' already exists partitioned - needs to be deleted first");
		}

	}

}
