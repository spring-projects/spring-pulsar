package org.springframework.pulsar.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.LogFactory;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.util.CollectionUtils;

public class PulsarAdministration implements ApplicationContextAware, SmartInitializingSingleton, PulsarAdminOperations {
	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(this.getClass()));

	private final PulsarAdminBuilder adminBuilder;

	private ApplicationContext applicationContext;

	public PulsarAdministration(Map<String, Object> adminConfig) {
		this.adminBuilder = PulsarAdmin.builder().loadConf(adminConfig);
	}

	public PulsarAdministration(PulsarAdminBuilder adminBuilder) {
		this.adminBuilder = adminBuilder;
	}

	@Override
	public void afterSingletonsInstantiated() {
		initialize();
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public void initialize() {
		Collection<PulsarTopic> topics = this.applicationContext.getBeansOfType(PulsarTopic.class, false, false).values();
		if (CollectionUtils.isEmpty(topics)) {
			return;
		}

		PulsarAdmin admin = null;
		try {
			admin = createAdminClient();
		} catch (Exception e) {
			throw new IllegalStateException("Could not create PulsarAdmin", e);
		}

		if (admin != null) {
			createOrModifyTopicsIfNeeded(admin, topics);
		}
	}

	private PulsarAdmin createAdminClient() throws PulsarClientException {
		return adminBuilder.build();
	}

	@Override
	public void createOrModifyTopics(PulsarTopic... topics) {
		PulsarAdmin admin;
		try {
			admin = createAdminClient();
		} catch (Exception e) {
			throw new IllegalStateException("Could not create PulsarAdmin", e);
		}

		if (admin != null) {
			createOrModifyTopicsIfNeeded(admin, Arrays.asList(topics));
		}
	}

	private Map<String, List<PulsarTopic>> getTopicsPerNamespace(Collection<PulsarTopic> topics) {
		Map<String, List<PulsarTopic>> topicsPerNamespace = new HashMap<>();
		topics.forEach(topic -> {
			PulsarTopic.TopicComponents topicComponents = topic.getComponents();
			String tenant = topicComponents.tenant();
			String namespace = topicComponents.namespace();
			String namespaceIdentifier = tenant + "/" + namespace;
			topicsPerNamespace.computeIfAbsent(namespaceIdentifier, k -> new ArrayList<>()).add(topic);
		});
		return topicsPerNamespace;
	}

	private List<String> getMatchingTopicPartitions(PulsarTopic topic, List<String> existingTopics) {
		return existingTopics
				.stream()
				.filter(existing -> existing.startsWith(topic.getFullyQualifiedTopicName() + "-partition-"))
				.toList();
	}

	private void createOrModifyTopicsIfNeeded(PulsarAdmin admin, Collection<PulsarTopic> topics) {
		if (CollectionUtils.isEmpty(topics)) {
			return;
		}

		Map<String, List<PulsarTopic>> topicsPerNamespace = getTopicsPerNamespace(topics);

		Set<PulsarTopic> topicsToCreate = new HashSet<>();
		Set<PulsarTopic> topicsToModify = new HashSet<>();

		topicsPerNamespace.forEach((namespace, requestedTopics) -> {
			try (admin) {
				List<String> existingTopicsInNamespace = admin.topics().getList(namespace);

				for (PulsarTopic topic : requestedTopics) {
					if (topic.isPartitioned()) {
						List<String> matchingPartitions = getMatchingTopicPartitions(topic, existingTopicsInNamespace);
						if (matchingPartitions.isEmpty()) {
							logger.info("Topic " + topic.getFullyQualifiedTopicName() + " does not exist.");
							topicsToCreate.add(topic);
						} else {
							int numberOfExistingPartitions = matchingPartitions.size();
							if (numberOfExistingPartitions < topic.numberOfPartitions()) {
								logger.info("Topic " + topic.getFullyQualifiedTopicName() + " found with "
										+ numberOfExistingPartitions + " partitions.");
								topicsToModify.add(topic);
							} else if (numberOfExistingPartitions > topic.numberOfPartitions()) {
								throw new IllegalStateException(
										"Topic " + topic.getFullyQualifiedTopicName() + " found with "
												+ numberOfExistingPartitions
												+ " partitions. Needs to be deleted first.");
							}
						}
					} else {
						if (!existingTopicsInNamespace.contains(topic.getFullyQualifiedTopicName())) {
							logger.info("Topic " + topic.getFullyQualifiedTopicName() + " does not exist.");
							topicsToCreate.add(topic);
						}
					}
				}

				createTopics(admin, topicsToCreate);
				modifyTopics(admin, topicsToModify);
			} catch (PulsarAdminException e) {
				throw new RuntimeException(e);
			}
		});
	}

	private void createTopics(PulsarAdmin admin, Set<PulsarTopic> topicsToCreate) throws PulsarAdminException {
		for (PulsarTopic topic : topicsToCreate) {
			if (topic.isPartitioned()) {
				admin.topics().createPartitionedTopic(topic.topicName(), topic.numberOfPartitions());
			} else {
				admin.topics().createNonPartitionedTopic(topic.topicName());
			}
		}
	}

	private void modifyTopics(PulsarAdmin admin, Set<PulsarTopic> topicsToModify) throws PulsarAdminException {
		for (PulsarTopic topic : topicsToModify) {
			admin.topics().updatePartitionedTopic(topic.topicName(), topic.numberOfPartitions());
		}
	}


	
	
}
