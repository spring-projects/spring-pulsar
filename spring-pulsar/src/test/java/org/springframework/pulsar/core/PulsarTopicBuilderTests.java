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
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.stream.Stream;

import org.apache.pulsar.common.naming.TopicDomain;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link PulsarTopicBuilder}.
 *
 * @author Chris Bono
 */
class PulsarTopicBuilderTests {

	private PulsarTopicBuilder builder = new PulsarTopicBuilder();

	@Test
	void whenNumPartitionsNotSpecifiedThenTopicIsNotPartitioned() {
		var topicName = "persistent://my-tenant/my-namespace/my-topic";
		var topic = builder.name(topicName).build();
		assertThat(topic.topicName()).isEqualTo(topicName);
		assertThat(topic.numberOfPartitions()).isEqualTo(0);
	}

	@Test
	void whenNumPartitionsSpecifiedThenTopicIsPartitioned() {
		var topicName = "persistent://my-tenant/my-namespace/my-topic";
		var topic = builder.name(topicName).numberOfPartitions(5).build();
		assertThat(topic.topicName()).isEqualTo(topicName);
		assertThat(topic.numberOfPartitions()).isEqualTo(5);
	}

	@ParameterizedTest
	@ValueSource(strings = { "persistent://my-namespace/my-topic", "my-namespace/my-topic" })
	void whenNameIsInvalidThenExceptionIsThrown(String invalidName) {
		assertThatIllegalArgumentException().isThrownBy(() -> builder.name(invalidName))
			.withMessage("Topic name '" + invalidName + "' must be in one of the following formats "
					+ "('name', 'tenant/namespace/name', 'domain://tenant/namespace/name')");
	}

	@ParameterizedTest
	@MethodSource("nameIsAlwaysFullyQualifiedProvider")
	void nameIsAlwaysFullyQualified(PulsarTopicBuilder topicBuilder, String inputTopic, String expectedTopic) {
		assertThat(topicBuilder.getFullyQualifiedNameForTopic(inputTopic)).isEqualTo(expectedTopic);
		var topic = topicBuilder.name(inputTopic).build();
		assertThat(topic.topicName()).isEqualTo(expectedTopic);
	}

	private static Stream<Arguments> nameIsAlwaysFullyQualifiedProvider() {
		var defaultBuilder = new PulsarTopicBuilder();
		var customBuilder = new PulsarTopicBuilder(TopicDomain.non_persistent, "my-tenant", "my-namespace");
		return Stream.of(Arguments.of(defaultBuilder, "my-topic", "persistent://public/default/my-topic"),
				Arguments.of(defaultBuilder, "foo/bar/my-topic", "persistent://foo/bar/my-topic"),
				Arguments.of(defaultBuilder, "non-persistent://foo/bar/my-topic", "non-persistent://foo/bar/my-topic"),
				Arguments.of(customBuilder, "my-topic", "non-persistent://my-tenant/my-namespace/my-topic"),
				Arguments.of(customBuilder, "foo/bar/my-topic", "non-persistent://foo/bar/my-topic"),
				Arguments.of(customBuilder, "persistent://foo/bar/my-topic", "persistent://foo/bar/my-topic"));
	}

	@Test
	void whenConstructedWithNullTenantThenPulsarDefaultTenantIsUsed() {
		var topicBuilder = new PulsarTopicBuilder(TopicDomain.persistent, null, "foo");
		var fqTopic = topicBuilder.name("my-topic").build();
		assertThat(fqTopic.topicName()).isEqualTo("persistent://public/foo/my-topic");
	}

	@Test
	void whenConstructedWithNullNamespaceThenPulsarDefaultNamespaceIsUsed() {
		var topicBuilder = new PulsarTopicBuilder(TopicDomain.persistent, "foo", null);
		var fqTopic = topicBuilder.name("my-topic").build();
		assertThat(fqTopic.topicName()).isEqualTo("persistent://foo/default/my-topic");
	}

}
