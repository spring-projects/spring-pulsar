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

package org.springframework.pulsar.reader;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.pulsar.annotation.PulsarReader;
import org.springframework.pulsar.annotation.PulsarReaderReaderBuilderCustomizer;
import org.springframework.pulsar.config.PulsarReaderEndpointRegistry;
import org.springframework.pulsar.reader.PulsarReaderSpelTests.IdAttribute.IdAttributeConfig;
import org.springframework.pulsar.reader.PulsarReaderSpelTests.ReaderCustomizerAttribute.ReaderCustomizerAttributeConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Tests {@code SpEL} functionality in {@link PulsarReader @PulsarReader} attributes.
 *
 * @author Chris Bono
 */
class PulsarReaderSpelTests extends PulsarReaderTestsBase {

	private static final String TOPIC = "pulsar-reader-spel-tests-topic";

	@Nested
	@ContextConfiguration(classes = IdAttributeConfig.class)
	@TestPropertySource(properties = "foo.id = foo")
	class IdAttribute {

		@Test
		void containerIdDerivedFromAttribute(@Autowired PulsarReaderEndpointRegistry registry) {
			assertThat(registry.getReaderContainer("foo")).isNotNull();
			assertThat(registry.getReaderContainer("bar")).isNotNull();
		}

		@EnablePulsar
		@Configuration(proxyBeanMethods = false)
		static class IdAttributeConfig {

			@PulsarReader(id = "${foo.id}", topics = TOPIC, startMessageId = "earliest")
			void listen1(String ignored) {
			}

			@PulsarReader(id = "#{T(java.lang.String).valueOf('bar')}", topics = TOPIC, startMessageId = "earliest")
			void listen2(String ignored) {
			}

		}

	}

	@Nested
	@ContextConfiguration(classes = ReaderCustomizerAttributeConfig.class)
	class ReaderCustomizerAttribute {

		@Test
		void readerCustomizerDerivedFromAttribute() {
			assertThat(ReaderCustomizerAttributeConfig.CUSTOMIZED_CONTAINERS_TOPIC_NAMES)
				.containsExactlyInAnyOrder("foo-topic", "bar-topic", "zaa-topic");
		}

		@EnablePulsar
		@Configuration(proxyBeanMethods = false)
		static class ReaderCustomizerAttributeConfig {

			static List<String> CUSTOMIZED_CONTAINERS_TOPIC_NAMES = new ArrayList<>();

			@SuppressWarnings("unchecked")
			@Bean
			PulsarReaderReaderBuilderCustomizer<?> customReaderCustomizer() {
				return (builder) -> {
					var conf = ReflectionTestUtils.getField(builder, "conf");
					var topicNames = (Set<String>) ReflectionTestUtils.getField(conf, "topicNames");
					CUSTOMIZED_CONTAINERS_TOPIC_NAMES.addAll(topicNames);
				};
			}

			@PulsarReader(id = "foo", readerCustomizer = "#{@customReaderCustomizer}", topics = "foo-topic",
					startMessageId = "earliest")
			void listen1(String ignored) {
			}

			@PulsarReader(id = "bar", readerCustomizer = "#{T(java.lang.String).valueOf('customReaderCustomizer')}",
					topics = "bar-topic", startMessageId = "earliest")
			void listen2(String ignored) {
			}

			@PulsarReader(id = "zaa", readerCustomizer = "customReaderCustomizer", topics = "zaa-topic",
					startMessageId = "earliest")
			void listen3(String ignored) {
			}

		}

	}

}
