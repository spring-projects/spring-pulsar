package org.springframework.pulsar.core;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.stream.Stream;
import org.apache.pulsar.common.naming.TopicDomain;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class PulsarTopicTests {

	@Test
	void builderDefaultValues() {
		String topicName = "test-default-values";
		PulsarTopicBuilder builder = PulsarTopic.builder(topicName);
		PulsarTopic topic = builder.build();

		assertThat(topic.topicName()).isEqualTo(topicName);
		assertThat(topic.numberOfPartitions()).isEqualTo(0);
	}

	@ParameterizedTest
	@MethodSource("topicComponentsProvider")
	void topicComponents(PulsarTopic topic, TopicDomain domain, String tenant, String namespace, String topicName) {
		PulsarTopic.TopicComponents components = topic.getComponents();
		assertThat(components.domain()).isEqualTo(domain);
		assertThat(components.tenant()).isEqualTo(tenant);
		assertThat(components.namespace()).isEqualTo(namespace);
		assertThat(components.topic()).isEqualTo(topicName);
	}

	private static Stream<Arguments> topicComponentsProvider() {
		return Stream.of(
				Arguments.of(PulsarTopic.builder("topic-1").build(),
						TopicDomain.persistent, "public", "default", "topic-1"),
				Arguments.of(PulsarTopic.builder("public/default/topic-2").build(),
						TopicDomain.persistent, "public", "default", "topic-2"),
				Arguments.of(PulsarTopic.builder("persistent://public/default/topic-3").build(),
						TopicDomain.persistent, "public", "default", "topic-3"),
				Arguments.of(PulsarTopic.builder("public/my-namespace/topic-4").build(),
						TopicDomain.persistent, "public", "my-namespace", "topic-4"),
				Arguments.of(PulsarTopic.builder("my-tenant/my-namespace/topic-5").build(),
						TopicDomain.persistent, "my-tenant", "my-namespace", "topic-5"),
				Arguments.of(PulsarTopic.builder("non-persistent://public/my-namespace/topic-6").build(),
						TopicDomain.non_persistent, "public", "my-namespace", "topic-6"),
				Arguments.of(PulsarTopic.builder("non-persistent://my-tenant/my-namespace/topic-7").build(),
						TopicDomain.non_persistent, "my-tenant", "my-namespace", "topic-7")
		);
	}

}
