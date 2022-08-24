package org.springframework.pulsar.core;

import java.util.Map;

public class PulsarTopicBuilder {

	private final String topicName;

	private int numberOfPartitions;


	protected PulsarTopicBuilder(String topicName) {
		this.topicName = topicName;
	}

	public PulsarTopicBuilder numberOfPartitions(int numberOfPartitions) {
		this.numberOfPartitions = numberOfPartitions;
		return this;
	}

	public PulsarTopic build() {
		return new PulsarTopic(this.topicName, this.numberOfPartitions);
	}
}
