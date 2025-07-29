/*
 * Copyright 2023-present the original author or authors.
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

import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.core.ProducerBuilderCustomizer;
import org.springframework.pulsar.support.JavaUtils;
import org.springframework.util.Assert;

/**
 * Log a warning when a Lambda is used as a producer builder customizer.
 * <p>
 * For each encountered Lambda customizer, the log is sampled on a frequency to avoid
 * flooding the logs. The warning is logged on the very first occurrence and then every
 * nth time thereafter.
 *
 * @author Chris Bono
 * @since 1.2.0
 */
public class LambdaCustomizerWarnLogger {

	private static final String CUSTOMIZER_LAMBDA_MSG = """
			Producer customizer [%s] is implemented as a Lambda. If you are experiencing write performance degradation
			it may be related to cache misses if the lambda is not following the rules outlined in
			https://docs.spring.io/spring-pulsar/reference/reference/pulsar/message-production.html#producer-caching-lambdas""";

	private final LogAccessor logger;

	private final EveryNthSampler<String> logSampler;

	/**
	 * Construct an instance.
	 * @param logger the backing logger
	 * @param frequency how often to log the warning (i.e. {@code every nth occurrence})
	 * @see EveryNthSampler
	 */
	public LambdaCustomizerWarnLogger(LogAccessor logger, long frequency) {
		Assert.notNull(logger, "logger must not be null");
		this.logger = logger;
		this.logSampler = new EveryNthSampler<>(frequency, 500);
	}

	/**
	 * Log a warning if the given customizer is implemented as a Lambda.
	 * <p>
	 * Note that the log is sampled and will be logged on the very first and every nth
	 * occurrence thereafter.
	 * @param producerCustomizer the customizer
	 */
	public void maybeLog(ProducerBuilderCustomizer<?> producerCustomizer) {
		var customizerClass = producerCustomizer.getClass();
		if (JavaUtils.INSTANCE.isLambda(customizerClass) && this.logSampler.trySample(customizerClass.getName())) {
			this.logger.warn(() -> CUSTOMIZER_LAMBDA_MSG.formatted(customizerClass.getName()));
		}
	}

}
