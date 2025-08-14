/*
 * Copyright 2022-present the original author or authors.
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

import java.util.regex.Pattern;

import org.apache.pulsar.common.naming.TopicDomain;

import org.springframework.util.Assert;

/**
 * Represents a Pulsar topic.
 * <p>
 * The input {@code topicName} must be fully-qualified. As such, it is recommended to use
 * the {@link PulsarTopicBuilder} to create instances like this: <pre>{@code
 * 	PulsarTopic topic = new PulsarTopicBuilder().name("my-topic").build();
 * }</pre> The builder is more lenient and allows non-fully-qualified topic names to be
 * input and fully qualifies the output name using its configured default tenant and
 * namepsace.
 *
 * @param topicName the fully qualified topic name in the format
 * {@code 'domain://tenant/namespace/name'}
 * @param numberOfPartitions the number of partitions, or 0 for non-partitioned topics
 * @author Alexander Preuß
 * @author Chris Bono
 * @see PulsarTopicBuilder
 */
public record PulsarTopic(String topicName, int numberOfPartitions) {

	// Pulsar allows (a-zA-Z_0-9) and special chars -=:. for names
	private static final String NAME_PATTERN_STR = "[-=:\\.\\w]*";

	private static Pattern TOPIC_NAME_PATTERN = Pattern.compile("(persistent|non-persistent)\\:\\/\\/(%s)\\/(%s)\\/(%s)"
		.formatted(NAME_PATTERN_STR, NAME_PATTERN_STR, NAME_PATTERN_STR));

	private static final String INVALID_NAME_MSG = "topicName %s must be fully-qualified "
			+ "in the format 'domain://tenant/namespace/name' where "
			+ "domain is one of ('persistent', 'non-persistent') and the other components must be "
			+ "composed of one or more letters, digits, or special characters ('-', '=', ':', or '.')";

	public PulsarTopic {
		Assert.state(TOPIC_NAME_PATTERN.matcher(topicName).matches(), INVALID_NAME_MSG.formatted(topicName));
		Assert.state(numberOfPartitions >= 0, "numberOfPartitions must be >= 0");
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
		var splitTopic = this.topicName().split("/");
		var type = splitTopic[0].replace(":", "");
		return new TopicComponents(TopicDomain.getEnum(type), splitTopic[2], splitTopic[3], splitTopic[4]);
	}

	/**
	 * Model class for the individual identifying components of a Pulsar topic.
	 *
	 * @param domain the topic domain
	 * @param tenant the topic tenant
	 * @param namespace the topic namespace
	 * @param name the topic name
	 */
	record TopicComponents(TopicDomain domain, String tenant, String namespace, String name) {

	}
}
