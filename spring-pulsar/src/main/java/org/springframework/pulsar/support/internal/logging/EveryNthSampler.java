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

package org.springframework.pulsar.support.internal.logging;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongPredicate;

import org.springframework.core.log.LogAccessor;
import org.springframework.util.Assert;

/**
 * Decides whether an input should be sampled based on frequency of occurrence.
 * <p>
 * Each input is considered sampled on the first and every nth time it is encountered.
 * <p>
 * The inputs are tracked in a bounded map whose entries are cleared when it reaches
 * capacity.
 *
 * @param <T> type of input (e.g. String)
 * @author Chris Bono
 * @since 1.2.0
 */
class EveryNthSampler<T> {

	private final long nth;

	private final long maxInputs;

	private final Map<T, AtomicLong> inputCounters = new ConcurrentHashMap<>();

	private final AtomicLong numInputCounters = new AtomicLong(0);

	private final LogAccessor logger = new LogAccessor(EveryNthSampler.class);

	/**
	 * Construct a sampler instance.
	 * @param nth the frequency (i.e. every nth occurrence)
	 * @param maxInputs the maximum number of inputs to track before clearing the map
	 */
	EveryNthSampler(long nth, long maxInputs) {
		Assert.state(nth > 0, () -> "nth must be a positive value");
		Assert.state(maxInputs > 0, () -> "maxInputs must be a positive value");
		this.nth = nth;
		this.maxInputs = maxInputs;
	}

	/**
	 * Determine if the given input should be sampled.
	 * <p>
	 * The input is considered sampled on the first and every nth occurrence.
	 * @param input the input to check
	 * @return whether the input should be sampled
	 */
	boolean trySample(T input) {
		var initialCounter = new AtomicLong(0);
		var inputCounter = this.inputCounters.computeIfAbsent(input, (__) -> initialCounter);
		// When new input added to map, increment and check size and clear if over max
		if (inputCounter == initialCounter) {
			this.incrementWithResetAtThreshold(this.numInputCounters, this.maxInputs + 1,
					Long.valueOf(this.maxInputs)::equals, () -> CompletableFuture.runAsync(() -> {
						this.logger.debug(() -> "Max inputs (%s) reached - clearing map".formatted(this.maxInputs));
						this.inputCounters.clear();
						this.inputCounters.computeIfAbsent(input, (__) -> initialCounter);
					}));
		}
		// Update input counter and mark sampled on 1st and Nth occurrence
		var sampled = new AtomicBoolean(false);
		this.incrementWithResetAtThreshold(inputCounter, this.nth, Long.valueOf(0L)::equals, () -> {
			this.logger.trace(() -> "Input [%s] is sampled".formatted(input));
			sampled.set(true);
		});
		return sampled.get();
	}

	private void incrementWithResetAtThreshold(AtomicLong counter, long threshold,
			LongPredicate runActionIfPriorCountMeets, Runnable action) {
		var priorCount = this.incrementWithResetAtThreshold(counter, threshold);
		if (runActionIfPriorCountMeets.test(priorCount)) {
			action.run();
		}
	}

	private long incrementWithResetAtThreshold(AtomicLong counter, long threshold) {
		return counter.getAndUpdate((currentCount) -> ((currentCount + 1) % threshold == 0) ? 0 : currentCount + 1);
	}

}
