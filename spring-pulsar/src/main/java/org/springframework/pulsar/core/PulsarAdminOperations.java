package org.springframework.pulsar.core;

public interface PulsarAdminOperations {

	void createOrModifyTopics(PulsarTopic... topics);

}
