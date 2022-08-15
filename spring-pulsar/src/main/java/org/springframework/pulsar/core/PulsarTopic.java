/*
 * Copyright 2022 the original author or authors.
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

import org.apache.pulsar.common.naming.TopicDomain;

/**
 * Model class for a Pulsar topic.
 *
 * Use the {@link PulsarTopicBuilder} to create instances like this:
 *
 * <pre>{@code
 * 	PulsarTopic topic = PulsarTopic.builder("topic-name").build();
 * }</pre>
 * @param topicName the topic name
 * @param numberOfPartitions the number of partitions, or 0 for non-partitioned topics
 *
 * @author Chris Bono
 * @author Alexander Preuß
 */
public record PulsarTopic(String topicName, int numberOfPartitions) {

	public static PulsarTopicBuilder builder(String topicName) {
		return new PulsarTopicBuilder(topicName);
	}

	/**
	 * Checks if the topic is partitioned.
	 * @return true if the topic is partitioned
	 */
	public boolean isPartitioned() {
		return this.numberOfPartitions != 0;
	}

	/**
	 * Get the individual identifying components of a Pulsar topic.
	 * @return {@link TopicComponents}
	 */
	public TopicComponents getComponents() {
		String[] splitTopic = this.topicName().split("/");
		if (splitTopic.length == 1) { // e.g. 'my-topic'
			return new TopicComponents(TopicDomain.persistent, "public", "default", splitTopic[0]);
		}
		else if (splitTopic.length == 3) { // e.g. 'public/default/my-topic'
			return new TopicComponents(TopicDomain.persistent, splitTopic[0], splitTopic[1], splitTopic[2]);
		}
		else if (splitTopic.length == 5) { // e.g. 'persistent://public/default/my-topic'
			String type = splitTopic[0].replace(":", "");
			return new TopicComponents(TopicDomain.getEnum(type), splitTopic[2], splitTopic[3], splitTopic[4]);
		}
		throw new IllegalArgumentException("Topic name '" + this + "' has unexpected components.");

	}

	/**
	 * Get the fully-qualified name of the topic.
	 * @return the fully-qualified topic name
	 */
	@Override
	public String toString() {
		TopicComponents components = this.getComponents();
		return components.domain + "://" + components.tenant + "/" + components.namespace + "/" + components.name;
	}

	/**
	 * Model class for the individual identifying components of a Pulsar topic.
	 * @param domain the topic domain
	 * @param tenant the topic tenant
	 * @param namespace the topic namespace
	 * @param name the topic name
	 */
	record TopicComponents(TopicDomain domain, String tenant, String namespace, String name) {

	}
}
