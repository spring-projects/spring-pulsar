package org.springframework.pulsar.core;

import java.util.Map;
import org.apache.pulsar.common.naming.TopicDomain;

public record PulsarTopic(String topicName, int numberOfPartitions) {

	public static PulsarTopicBuilder builder(String topicName) {
		return new PulsarTopicBuilder(topicName);
	}

	public String getFullyQualifiedTopicName() {
		return this.getComponents().toString();

	}

	public boolean isPartitioned() {
		return this.numberOfPartitions != 0;
	}

	public TopicComponents getComponents() {
		String[] splitTopic = this.topicName().split("/");
		if (splitTopic.length == 1) { // looks like 'my-topic'
			return new TopicComponents(TopicDomain.persistent, "public", "default", splitTopic[0]);
		} else if (splitTopic.length == 3) { // looks like 'public/default/my-topic'
			return new TopicComponents(TopicDomain.persistent, splitTopic[0], splitTopic[1], splitTopic[2]);
		} else if (splitTopic.length == 5) { // looks like 'persistent://public/default/my-topic'
			String type = splitTopic[0].substring(0, splitTopic[0].length() - 1); // remove ':'
			return new TopicComponents(TopicDomain.getEnum(type), splitTopic[2], splitTopic[3], splitTopic[4]);
		}
		throw new IllegalArgumentException("Topic name '" + this + "' has unexpected components.");

	}

	record TopicComponents(TopicDomain domain, String tenant, String namespace, String topic) {
		@Override
		public String toString() {
			return this.domain + "://" + this.tenant + "/" + this.namespace + "/" + this.topic;
		}
	}
}
