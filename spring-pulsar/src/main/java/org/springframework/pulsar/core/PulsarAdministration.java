package org.springframework.pulsar.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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
import org.springframework.core.log.LogMessage;
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
		createOrModifyTopicsIfNeeded(topics);
	}

	private PulsarAdmin createAdminClient() throws PulsarClientException {
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
		return existingTopics
				.stream()
				.filter(existing -> existing.startsWith(topic.getFullyQualifiedTopicName() + "-partition-"))
				.toList();
	}

	private void createOrModifyTopicsIfNeeded(Collection<PulsarTopic> topics) {
		if (CollectionUtils.isEmpty(topics)) {
			return;
		}

		PulsarAdmin admin;
		try {
			admin = createAdminClient();
		} catch (Exception e) {
			throw new IllegalStateException("Could not create PulsarAdmin", e);
		}

		Map<String, List<PulsarTopic>> topicsPerNamespace = getTopicsPerNamespace(topics);

		Set<PulsarTopic> topicsToCreate = new HashSet<>();
		Set<PulsarTopic> topicsToModify = new HashSet<>();

		topicsPerNamespace.forEach((namespace, requestedTopics) -> {
			try {
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
		admin.close();
	}

	private void createTopics(PulsarAdmin admin, Set<PulsarTopic> topicsToCreate) throws PulsarAdminException {
		logger.debug("Creating topics: "  + topicsToCreate.stream().map(PulsarTopic::getFullyQualifiedTopicName).collect(Collectors.joining(",")));
		for (PulsarTopic topic : topicsToCreate) {
			if (topic.isPartitioned()) {
				admin.topics().createPartitionedTopic(topic.topicName(), topic.numberOfPartitions());
			} else {
				admin.topics().createNonPartitionedTopic(topic.topicName());
			}
		}
	}

	private void modifyTopics(PulsarAdmin admin, Set<PulsarTopic> topicsToModify) throws PulsarAdminException {
		logger.debug("Modifying topics: "  + topicsToModify.stream().map(PulsarTopic::getFullyQualifiedTopicName).collect(Collectors.joining(",")));
		for (PulsarTopic topic : topicsToModify) {
			admin.topics().updatePartitionedTopic(topic.topicName(), topic.numberOfPartitions());
		}
	}


	
	
}
