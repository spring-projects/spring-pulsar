/*
 * Copyright 2024-present the original author or authors.
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

package org.springframework.pulsar.inttest.logging;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.regex.Pattern;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.pulsar.core.ProducerBuilderCustomizer;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.PulsarTemplateCustomizer;
import org.springframework.pulsar.inttest.logging.PulsarTemplateLambdaWarnLoggerTests.WithWarnLoggerDisabled.WithWarnLoggerDisabledConfig;
import org.springframework.pulsar.inttest.logging.PulsarTemplateLambdaWarnLoggerTests.WithWarnLoggerIncreasedFrequency.WithWarnLoggerIncreasedFrequencyConfig;
import org.springframework.pulsar.support.internal.logging.LambdaCustomizerWarnLogger;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;

/**
 * Integration tests that covers {@link LambdaCustomizerWarnLogger} and its usage in
 * {@link PulsarTemplate} of the following cases: <pre>
 *  - user customizes the template to disable the warn logger
 *  - user customizes the template to adjust the warn logger frequency
 *  - template logs warning when a lambda customizer is used as producer customizer
 *  - template does not log warning when a non-lambda customizer is used
 * </pre> The nature of the feature (logging and template customization) lends itself well
 * to an integration test w/ help of {@link CapturedOutput} and
 * {@link PulsarTemplateCustomizer}.
 *
 * @author Chris Bono
 */
@Testcontainers(disabledWithoutDocker = true)
@ExtendWith(OutputCaptureExtension.class)
class PulsarTemplateLambdaWarnLoggerTests {

	@SuppressWarnings("unused")
	@Container
	@ServiceConnection
	static PulsarContainer PULSAR_CONTAINER = new PulsarContainer(PulsarTestContainerSupport.getPulsarImage());

	@Nested
	@SpringBootTest(classes = TestAppConfig.class)
	@ExtendWith(OutputCaptureExtension.class)
	class WithWarnLoggerEnabledByDefault {

		@Test
		void whenLambdaCustomizerIsUsedThenWarningIsLogged(CapturedOutput output,
				@Autowired PulsarTemplate<String> template) {
			TestUtils.sendRequestsWithCustomizer("lcwlt-default", 1001, template);
			TestUtils.assertThatWarningIsLoggedNumTimes(2, output);
		}

	}

	@Nested
	@SpringBootTest(classes = { TestAppConfig.class, WithWarnLoggerIncreasedFrequencyConfig.class })
	@ExtendWith(OutputCaptureExtension.class)
	class WithWarnLoggerIncreasedFrequency {

		@Test
		void whenLambdaCustomizerIsUsedThenWarningIsLoggedMoreFrequently(CapturedOutput output,
				@Autowired PulsarTemplate<String> template) {
			TestUtils.sendRequestsWithCustomizer("lcwlt-adjusted", 1001, template);
			TestUtils.assertThatWarningIsLoggedNumTimes(11, output);
		}

		@TestConfiguration(proxyBeanMethods = false)
		static class WithWarnLoggerIncreasedFrequencyConfig {

			@Bean
			PulsarTemplateCustomizer<?> templateCustomizer() {
				return (template) -> template.logWarningForLambdaCustomizer(100);
			}

		}

	}

	@Nested
	@SpringBootTest(classes = { TestAppConfig.class, WithWarnLoggerDisabledConfig.class })
	@ExtendWith(OutputCaptureExtension.class)
	class WithWarnLoggerDisabled {

		@Test
		void whenLambdaCustomizerIsUsedThenWarningIsNotLogged(CapturedOutput output,
				@Autowired PulsarTemplate<String> template) {
			TestUtils.sendRequestsWithCustomizer("lcwlt-disabled", 1001, template);
			TestUtils.assertThatWarningIsLoggedNumTimes(0, output);
		}

		@TestConfiguration(proxyBeanMethods = false)
		static class WithWarnLoggerDisabledConfig {

			@Bean
			PulsarTemplateCustomizer<?> templateCustomizer() {
				return (template) -> template.logWarningForLambdaCustomizer(0);
			}

		}

	}

	@Nested
	@SpringBootTest(classes = TestAppConfig.class, properties = "spring.pulsar.producer.cache.enabled=false")
	@ExtendWith(OutputCaptureExtension.class)
	class WithNonCachingProducerFactory {

		@Test
		void whenLambdaCustomizerIsUsedThenWarningIsNotLogged(CapturedOutput output,
				@Autowired PulsarTemplate<String> template) {
			TestUtils.sendRequestsWithCustomizer("lcwlt-non-caching", 1001, template);
			TestUtils.assertThatWarningIsLoggedNumTimes(0, output);
		}

	}

	@SpringBootConfiguration
	@EnableAutoConfiguration
	static class TestAppConfig {

	}

	private static class TestUtils {

		private static void sendRequestsWithCustomizer(String testPrefix, int numberOfSends,
				PulsarTemplate<String> template) {
			sendRequests(testPrefix, numberOfSends, (pb) -> {
			}, template);
		}

		private static void sendRequestsWithoutCustomizer(String testPrefix, int numberOfSends,
				PulsarTemplate<String> template) {
			sendRequests(testPrefix, numberOfSends, null, template);
		}

		private static void sendRequests(String testPrefix, int numberOfSends,
				ProducerBuilderCustomizer<String> customizer, PulsarTemplate<String> template) {
			for (int i = 0; i < numberOfSends; i++) {
				var msg = "LambdaCustomizerWarningLog-i:" + i;
				var builder = template.newMessage(msg).withTopic("%s-topic".formatted(testPrefix));
				if (customizer != null) {
					builder.withProducerCustomizer(customizer);
				}
				builder.send();
			}
		}

		private static void assertThatWarningIsLoggedNumTimes(int expectedNumberOfTimes, CapturedOutput output) {
			// pause to make sure log is flushed to console before checking (sanity)
			try {
				Thread.sleep(1000);
			}
			catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			var pattern = Pattern.compile("(Producer customizer \\[.+?\\] is implemented as a Lambda)");
			assertThat(output.getAll())
				.satisfies((outputStr) -> assertThat(pattern.matcher(outputStr).results().count())
					.isEqualTo(expectedNumberOfTimes));
		}

	}

}
