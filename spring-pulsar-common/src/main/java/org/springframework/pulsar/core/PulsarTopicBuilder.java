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

/**
 * Builder class to create {@link PulsarTopic} instances.
 *
 * @author Alexander Preuß
 */
public class PulsarTopicBuilder {

	private final String topicName;

	private int numberOfPartitions;

	protected PulsarTopicBuilder(String topicName) {
		this.topicName = topicName;
	}

	/**
	 * Sets the number of topic partitions.
	 * @param numberOfPartitions the number of topic partitions
	 * @return this builder
	 */
	public PulsarTopicBuilder numberOfPartitions(int numberOfPartitions) {
		this.numberOfPartitions = numberOfPartitions;
		return this;
	}

	/**
	 * Constructs the {@link PulsarTopic} with the properties configured in this builder.
	 * @return {@link PulsarTopic}
	 */
	public PulsarTopic build() {
		return new PulsarTopic(this.topicName, this.numberOfPartitions);
	}

}
