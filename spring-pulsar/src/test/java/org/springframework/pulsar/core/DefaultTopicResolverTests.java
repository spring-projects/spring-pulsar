/*
 * Copyright 2023 the original author or authors.
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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.stream.Stream;

import org.apache.pulsar.common.schema.SchemaType;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.lang.Nullable;
import org.springframework.pulsar.annotation.PulsarTypeMapping;

/**
 * Unit tests for {@link DefaultTopicResolver}.
 *
 * @author Chris Bono
 * @author Aleksei Arsenev
 */
class DefaultTopicResolverTests {

	private static final String userTopic = "user-topic1";

	private static final String defaultTopic = "default-topic1";

	private static final String fooTopic = "foo-topic1";

	private static final String bazTopic = "baz-topic1";

	private static final String stringTopic = "string-topic1";

	private DefaultTopicResolver resolver = new DefaultTopicResolver();

	@BeforeEach
	void addMappingsToResolver() {
		resolver.addCustomTopicMapping(Foo.class, fooTopic);
		resolver.addCustomTopicMapping(String.class, stringTopic);
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("resolveNoMessageInfoProvider")
	void resolveNoMessageInfo(String testName, @Nullable String userTopic, @Nullable String defaultTopic,
			@Nullable String expectedTopic) {
		assertThat(resolver.resolveTopic(userTopic, () -> defaultTopic).value().orElse(null)).isEqualTo(expectedTopic);
	}

	static Stream<Arguments> resolveNoMessageInfoProvider() {
		// @formatter:off
		return Stream.of(
				arguments("userTopicWithDefault", userTopic, defaultTopic, userTopic),
				arguments("userTopicNoDefault", userTopic, null, userTopic),
				arguments("noUserTopicWithDefault", null, defaultTopic, defaultTopic),
				arguments("noUserTopicNoDefault", null, null, null)
		);
		// @formatter:on
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("resolveByMessageInstanceProvider")
	<T> void resolveByMessageInstance(String testName, @Nullable String userTopic, T message,
			@Nullable String defaultTopic, @Nullable String expectedTopic) {
		assertThat(resolver.resolveTopic(userTopic, message, () -> defaultTopic).value().orElse(null))
			.isEqualTo(expectedTopic);
	}

	static Stream<Arguments> resolveByMessageInstanceProvider() {
		// @formatter:off
		return Stream.of(
				arguments("primitiveMessageWithUserTopic", userTopic, "strMessage", defaultTopic, userTopic),
				arguments("primitiveMessageNoUserTopic", null, "strMessage", defaultTopic, stringTopic),
				arguments("complexMessageWithUserTopic", userTopic, new Foo("5150"), defaultTopic, userTopic),
				arguments("complexMessageNoUserTopic", null, new Foo("5150"), defaultTopic, fooTopic),
				arguments("noMatchWithUserTopicAndDefault", userTopic, new Bar("123"), defaultTopic, userTopic),
				arguments("noMatchWithUserTopic", userTopic, new Bar("123"), null, userTopic),
				arguments("noMatchWithDefault", null, new Bar("123"), defaultTopic, defaultTopic),
				arguments("noMatchNoUserTopicNorDefault", null, new Bar("123"), null, null)
		);
		// @formatter:on
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("resolveByMessageTypeProvider")
	void resolveByMessageType(String testName, @Nullable String userTopic, Class<?> messageType,
			@Nullable String defaultTopic, @Nullable String expectedTopic) {
		assertThat(resolver.resolveTopic(userTopic, messageType, () -> defaultTopic).value().orElse(null))
			.isEqualTo(expectedTopic);
	}

	static Stream<Arguments> resolveByMessageTypeProvider() {
		// @formatter:off
		return Stream.of(
				arguments("primitiveMessageWithUserTopic", userTopic, String.class, defaultTopic, userTopic),
				arguments("primitiveMessageNoUserTopic", null, String.class, defaultTopic, stringTopic),
				arguments("complexMessageWithUserTopic", userTopic, Foo.class, defaultTopic, userTopic),
				arguments("complexMessageNoUserTopic", null, Foo.class, defaultTopic, fooTopic),
				arguments("nullMessageWithUserTopicAndDefault", userTopic, null, defaultTopic, userTopic),
				arguments("annotationMessageWithUserTopic", userTopic, Baz.class, defaultTopic, userTopic),
				arguments("annotationMessageNoUserTopic", null, Baz.class, defaultTopic, bazTopic),
				arguments("annotationMessageNoTopicInfo", null, BazNoTopicInfo.class, defaultTopic, defaultTopic),
				arguments("nullMessageWithDefault", null, null, defaultTopic, null),
				arguments("noMatchWithUserTopicAndDefault", userTopic, Bar.class, defaultTopic, userTopic),
				arguments("noMatchWithUserTopic", userTopic, Bar.class, null, userTopic),
				arguments("noMatchWithDefault", null, Bar.class, defaultTopic, defaultTopic),
				arguments("noMatchNoUserTopicNorDefault", null, Bar.class, null, null)
		);
		// @formatter:on
	}

	@Nested
	class TopicByAnnotatedMessageType {

		@Test
		void customMappingTakesPrecedenceOverAnnotationMapping() {
			assertThat(resolver.resolveTopic(null, Baz.class, () -> defaultTopic).value().orElse(null))
				.isEqualTo(bazTopic);
			resolver.addCustomTopicMapping(Baz.class, "baz-custom-topic");
			assertThat(resolver.resolveTopic(null, Baz.class, () -> defaultTopic).value().orElse(null))
				.isEqualTo("baz-custom-topic");
		}

		@Test
		void annotationMappingIgnoredWhenFeatureDisabled() {
			resolver.usePulsarTypeMappingAnnotations(false);
			assertThat(resolver.resolveTopic(null, Baz.class, () -> defaultTopic).value().orElse(null))
				.isEqualTo(defaultTopic);
		}

		@Test
		void annotatedMessageTypeWithTopicInfo() {
			resolver = spy(resolver);
			assertThat(resolver.resolveTopic(null, Baz.class, () -> defaultTopic).value().orElse(null))
				.isEqualTo(bazTopic);
			// verify added to custom mappings
			assertThat(resolver.getCustomTopicMappings().get(Baz.class)).isEqualTo(bazTopic);
			// verify subsequent calls skip resolution again
			assertThat(resolver.resolveTopic(null, Baz.class, () -> defaultTopic).value().orElse(null))
				.isEqualTo(bazTopic);
			verify(resolver, times(1)).getAnnotatedTopicInfo(Baz.class);
		}

	}

	@Nested
	class TopicMappingsAPI {

		@BeforeEach
		void resetResolver() {
			resolver = new DefaultTopicResolver();
		}

		@Test
		void noMappingsByDefault() {
			resolver = new DefaultTopicResolver();
			assertThat(resolver.getCustomTopicMappings()).asInstanceOf(InstanceOfAssertFactories.MAP).isEmpty();
		}

		@Test
		void addMappings() {
			String topic1 = fooTopic;
			String topic2 = "bar-topic";
			String previouslyMappedTopic = resolver.addCustomTopicMapping(Foo.class, topic1);
			assertThat(previouslyMappedTopic).isNull();
			assertThat(resolver.getCustomTopicMappings()).asInstanceOf(InstanceOfAssertFactories.MAP)
				.containsEntry(Foo.class, topic1);
			previouslyMappedTopic = resolver.addCustomTopicMapping(Foo.class, topic2);
			assertThat(previouslyMappedTopic).isEqualTo(topic1);
			assertThat(resolver.getCustomTopicMappings()).asInstanceOf(InstanceOfAssertFactories.MAP)
				.containsEntry(Foo.class, topic2);
		}

		@Test
		void removeMappings() {
			String previouslyMappedTopic = resolver.removeCustomMapping(Foo.class);
			assertThat(previouslyMappedTopic).isNull();
			resolver.addCustomTopicMapping(Foo.class, fooTopic);
			previouslyMappedTopic = resolver.removeCustomMapping(Foo.class);
			assertThat(previouslyMappedTopic).isEqualTo(fooTopic);
			assertThat(resolver.getCustomTopicMappings()).asInstanceOf(InstanceOfAssertFactories.MAP).isEmpty();
		}

	}

	record Foo(String value) {
	}

	record Bar(String value) {
	}

	@PulsarTypeMapping(topic = bazTopic)
	record Baz(String value) {
	}

	@PulsarTypeMapping(schemaType = SchemaType.STRING)
	record BazNoTopicInfo(String value) {
	}

}
