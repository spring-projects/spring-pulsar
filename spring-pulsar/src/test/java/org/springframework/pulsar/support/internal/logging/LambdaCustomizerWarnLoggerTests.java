/*
 * Copyright 2024-2024 the original author or authors.
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

package org.springframework.pulsar.support.internal.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.function.Supplier;

import org.apache.pulsar.client.api.ProducerBuilder;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;

import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.core.ProducerBuilderCustomizer;

/**
 * Tests for {@link LambdaCustomizerWarnLogger}.
 */
class LambdaCustomizerWarnLoggerTests {

	@Test
	void whenConstructedThenSamplerCreatedWithFrequency() {
		var logger = new LogAccessor(this.getClass());
		var warnLogger = new LambdaCustomizerWarnLogger(logger, 5);
		assertThat(warnLogger).extracting("logSampler", InstanceOfAssertFactories.type(EveryNthSampler.class))
			.satisfies((sampler) -> {
				assertThat(sampler).hasFieldOrPropertyWithValue("nth", 5L);
				assertThat(sampler).hasFieldOrPropertyWithValue("maxInputs", 500L);
			});
	}

	@Test
	void whenCustomizerIsLambdaThenWarningIsLogged() {
		var logger = mock(LogAccessor.class);
		var warnLogger = new LambdaCustomizerWarnLogger(logger, 5);
		ProducerBuilderCustomizer<String> lambdaCustomizer = (__) -> {
		};
		warnLogger.maybeLog(lambdaCustomizer);
		var logMsgPrefix = "Producer customizer [%s] is implemented as a Lambda."
			.formatted(lambdaCustomizer.getClass().getName());
		ArgumentMatcher<Supplier<String>> argMatcher = (s) -> s.get().startsWith(logMsgPrefix);
		verify(logger).warn(argThat(argMatcher));
	}

	@Test
	void whenCustomizerIsNotLambdaThenWarningIsNotLogged() {
		var logger = mock(LogAccessor.class);
		var warnLogger = new LambdaCustomizerWarnLogger(logger, 5);
		warnLogger.maybeLog(new NonLambdaCustomizer());
		verifyNoInteractions(logger);
	}

	static class NonLambdaCustomizer implements ProducerBuilderCustomizer<String> {

		@Override
		public void customize(ProducerBuilder<String> producerBuilder) {

		}

	}

}
