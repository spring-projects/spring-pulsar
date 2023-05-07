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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

/**
 * An administration class that delegates to {@link PulsarAdmin} to create and manage
 * topics defined in the application context.
 *
 * @author Alexander Preuß
 * @author Chris Bono
 * @author Kirill Merkushev
 */
public class PulsarAdministration
		implements ApplicationContextAware, SmartInitializingSingleton, PulsarAdministrationOperations {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	@Nullable
	private ApplicationContext applicationContext;

	@Nullable
	private final PulsarAdminBuilderCustomizer adminCustomizer;

	/**
	 * Construct a default instance using the specified service url.
	 * @param serviceHttpUrl the admin http service url
	 */
	public PulsarAdministration(String serviceHttpUrl) {
		this((adminBuilder) -> adminBuilder.serviceHttpUrl(serviceHttpUrl));
	}

	/**
	 * Construct an instance with the specified customizations.
	 * @param adminCustomizer the customizer to apply to the builder or null to use the
	 * default admin builder without modifications
	 */
	public PulsarAdministration(@Nullable PulsarAdminBuilderCustomizer adminCustomizer) {
		this.adminCustomizer = adminCustomizer;
	}

	@Override
	public void afterSingletonsInstantiated() {
		initialize();
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	private void initialize() {
		var topics = Objects.requireNonNull(this.applicationContext, "Application context was not set")
				.getBeansOfType(PulsarTopic.class, false, false).values();
		createOrModifyTopicsIfNeeded(topics);
	}

	public PulsarAdmin createAdminClient() throws PulsarClientException {
		var adminBuilder = PulsarAdmin.builder();
		if (this.adminCustomizer != null) {
			this.adminCustomizer.customize(adminBuilder);
		}
		return adminBuilder.build();
	}

	@Override
	public void createOrModifyTopics(PulsarTopic... topics) {
		createOrModifyTopicsIfNeeded(Arrays.asList(topics));
	}

	private Map<String, List<PulsarTopic>> getTopicsPerNamespace(Collection<PulsarTopic> topics) {
		return topics.stream().collect(Collectors.groupingBy(this::getTopicNamespaceIdentifier));
	}

	private String getTopicNamespaceIdentifier(PulsarTopic topic) {
		return topic.getComponents().tenant() + "/" + topic.getComponents().namespace();
	}

	private List<String> getMatchingTopicPartitions(PulsarTopic topic, List<String> existingTopics) {
		return existingTopics.stream()
				.filter(existing -> existing.startsWith(topic.getFullyQualifiedTopicName() + "-partition-")).toList();
	}

	private void createOrModifyTopicsIfNeeded(Collection<PulsarTopic> topics) {
		if (CollectionUtils.isEmpty(topics)) {
			return;
		}

		try (PulsarAdmin admin = createAdminClient()) {
			doCreateOrModifyTopicsIfNeeded(admin, topics);
		}
		catch (PulsarClientException e) {
			throw new IllegalStateException("Could not create PulsarAdmin", e);
		}
	}

	private void doCreateOrModifyTopicsIfNeeded(PulsarAdmin admin, Collection<PulsarTopic> topics) {
		var topicsPerNamespace = getTopicsPerNamespace(topics);

		topicsPerNamespace.forEach((namespace, requestedTopics) -> {
			var topicsToCreate = new HashSet<PulsarTopic>();
			var topicsToModify = new HashSet<PulsarTopic>();

			try {
				var existingTopicsInNamespace = admin.topics().getList(namespace);

				for (var topic : requestedTopics) {
					var topicName = topic.getFullyQualifiedTopicName();
					if (topic.isPartitioned()) {
						if (existingTopicsInNamespace.contains(topicName)) {
							throw new IllegalStateException(
									"Topic '%s' already exists un-partitioned - needs to be deleted first"
											.formatted(topicName));
						}
						var matchingPartitions = getMatchingTopicPartitions(topic, existingTopicsInNamespace);
						if (matchingPartitions.isEmpty()) {
							this.logger.debug(() -> "Topic '%s' does not yet exist - will add".formatted(topicName));
							topicsToCreate.add(topic);
						}
						else {
							var numberOfExistingPartitions = matchingPartitions.size();
							if (numberOfExistingPartitions < topic.numberOfPartitions()) {
								this.logger.debug(() -> "Topic '%s' found with %d partitions - will update to %d"
										.formatted(topicName, numberOfExistingPartitions, topic.numberOfPartitions()));
								topicsToModify.add(topic);
							}
							else if (numberOfExistingPartitions > topic.numberOfPartitions()) {
								throw new IllegalStateException(
										"Topic '%s' found w/ %d partitions but can't shrink to %d - needs to be deleted first"
												.formatted(topicName, numberOfExistingPartitions,
														topic.numberOfPartitions()));
							}
						}
					}
					else {
						var matchingPartitions = getMatchingTopicPartitions(topic, existingTopicsInNamespace);
						if (!matchingPartitions.isEmpty()) {
							throw new IllegalStateException(
									"Topic '%s' already exists partitioned - needs to be deleted first"
											.formatted(topicName));
						}
						if (!existingTopicsInNamespace.contains(topicName)) {
							this.logger.debug(() -> "Topic '%s' does not yet exist - will add".formatted(topicName));
							topicsToCreate.add(topic);
						}
					}
				}

				createTopics(admin, topicsToCreate);
				modifyTopics(admin, topicsToModify);
			}
			catch (PulsarAdminException e) {
				throw new RuntimeException(e);
			}
		});
	}

	private void createTopics(PulsarAdmin admin, Set<PulsarTopic> topicsToCreate) throws PulsarAdminException {
		this.logger.debug(() -> "Creating topics: " + topicsToCreate.stream()
				.map(PulsarTopic::getFullyQualifiedTopicName).collect(Collectors.joining(",")));
		for (var topic : topicsToCreate) {
			if (topic.isPartitioned()) {
				admin.topics().createPartitionedTopic(topic.topicName(), topic.numberOfPartitions());
			}
			else {
				admin.topics().createNonPartitionedTopic(topic.topicName());
			}
		}
	}

	private void modifyTopics(PulsarAdmin admin, Set<PulsarTopic> topicsToModify) throws PulsarAdminException {
		this.logger.debug(() -> "Modifying topics: " + topicsToModify.stream()
				.map(PulsarTopic::getFullyQualifiedTopicName).collect(Collectors.joining(",")));
		for (var topic : topicsToModify) {
			admin.topics().updatePartitionedTopic(topic.topicName(), topic.numberOfPartitions());
		}
	}

}
