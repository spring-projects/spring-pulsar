package org.springframework.pulsar.core;

import java.util.Map;

public class PulsarTopicBuilder {

	private String topicName;

	private int numberOfPartitions;

	private Map<String, String> properties;


	protected PulsarTopicBuilder(String topicName) {
		this.topicName = topicName;
	}

	public PulsarTopicBuilder setNumberOfPartitions(int numberOfPartitions) {
		this.numberOfPartitions = numberOfPartitions;
		return this;
	}

	public PulsarTopicBuilder setProperties(Map<String, String> properties) {
		this.properties = properties;
		return this;
	}

	public PulsarTopic build() {
		return new PulsarTopic(this.topicName, this.numberOfPartitions, this.properties);
	}
}
