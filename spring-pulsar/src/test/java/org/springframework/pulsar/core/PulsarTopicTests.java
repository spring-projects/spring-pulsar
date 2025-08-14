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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import org.apache.pulsar.common.naming.TopicDomain;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link PulsarTopic}.
 *
 * @author Alexander Preuß
 * @@author Chris Bono
 */
public class PulsarTopicTests {

	private static final String FULLY_QUALIFIED_TOPIC = "persistent://public/default/my-topic";

	@Test
	void whenNegativeNumPartitionsThenExceptionIsThrown() {
		assertThatIllegalStateException().isThrownBy(() -> new PulsarTopic(FULLY_QUALIFIED_TOPIC, -1))
			.withMessage("numberOfPartitions must be >= 0");
	}

	@Test
	void whenZeroNumPartitionsThenTopicIsNotPartitioned() {
		var topic = new PulsarTopic(FULLY_QUALIFIED_TOPIC, 0);
		assertThat(topic.numberOfPartitions()).isEqualTo(0);
		assertThat(topic.isPartitioned()).isFalse();
	}

	@Test
	void whenPosititveNumPartitionsThenTopicIsPartitioned() {
		var topic = new PulsarTopic(FULLY_QUALIFIED_TOPIC, 2);
		assertThat(topic.numberOfPartitions()).isEqualTo(2);
		assertThat(topic.isPartitioned()).isTrue();
	}

	@ParameterizedTest
	// @formatter:off
	@ValueSource(strings = {
			"my-domain://public/default/my-topic",
			"public/default/my-topic", "my-topic",
			"persistent://public/cluster/default/my-topic",
			"persistent://publ@c/default/my-topic",
			"persistent://public/def@ult/my-topic",
			"persistent://public/default/my-t@pic"
	})
	// @formatter:on
	void whenNameIsInvalidThenExceptionIsThrown(String invalidTopicName) {
		var msg = "topicName %s must be fully-qualified in the format".formatted(invalidTopicName);
		assertThatIllegalStateException().isThrownBy(() -> new PulsarTopic(invalidTopicName, 0))
			.withMessageStartingWith(msg);
	}

	@ParameterizedTest
	// @formatter:off
	@ValueSource(strings = {
			"persistent://public/default/my-topic",
			"non-persistent://public/default/my-topic",
			"persistent://PUB-=:.7lic/DE-=:.7fault/MY-=:.7topic"
	})
	// @formatter:on
	void whenNameIsValidThenTopicCreated(String validTopicName) {
		var topic = new PulsarTopic(validTopicName, 0);
		assertThat(topic.topicName()).isEqualTo(validTopicName);
	}

	@Test
	void getComponentsReturnsProperComponents() {
		var topic = new PulsarTopic("persistent://public/default/my-topic", 0);
		var components = topic.getComponents();
		assertThat(components.domain()).isEqualTo(TopicDomain.persistent);
		assertThat(components.tenant()).isEqualTo("public");
		assertThat(components.namespace()).isEqualTo("default");
		assertThat(components.name()).isEqualTo("my-topic");
	}

}
