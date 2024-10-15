/*
 * Copyright 2023-2024 the original author or authors.
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
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link EveryNthSampler}.
 */
class EveryNthSamplerTests {

	@Test
	void singleInputInvokesEveryNth() {
		var counter = new AtomicInteger(0);
		var sampler = new EveryNthSampler<String>(5, 1000);
		var input1 = "abc";
		// invoke on 1st
		incrementCounterIfSampled(counter, sampler, input1);
		assertThat(counter).hasValue(1);
		// do not invoke on 2nd-4th
		IntStream.range(0, 4).forEach((__) -> incrementCounterIfSampled(counter, sampler, input1));
		assertThat(counter).hasValue(1);
		// invoke on 5th
		incrementCounterIfSampled(counter, sampler, input1);
		assertThat(counter).hasValue(2);
		// do not invoke on 6th-9th
		IntStream.range(0, 4).forEach((__) -> incrementCounterIfSampled(counter, sampler, input1));
		assertThat(counter).hasValue(2);
		// invoke on 10th
		incrementCounterIfSampled(counter, sampler, input1);
		assertThat(counter).hasValue(3);
	}

	@Test
	void multiInputsInvokeEveryNth() {
		var input1 = "abc";
		var input2 = "def";
		var counter1 = new AtomicInteger(0);
		var counter2 = new AtomicInteger(0);
		var sampler = new EveryNthSampler<String>(5, 1000);
		IntStream.range(0, 20).forEach((__) -> {
			incrementCounterIfSampled(counter1, sampler, input1);
			incrementCounterIfSampled(counter2, sampler, input2);
		});
		assertThat(counter1).hasValue(4);
		assertThat(counter2).hasValue(4);
	}

	@Test
	void invokeOnEveryCall() {
		var counter = new AtomicInteger(0);
		var sampler = new EveryNthSampler<String>(1, 1000);
		IntStream.range(0, 10).forEach((__) -> incrementCounterIfSampled(counter, sampler, "abc"));
		assertThat(counter).hasValue(10);
	}

	@Test
	void whenMaxInputsThenMapIsCleared() {
		var input1 = "aaa";
		var input2 = "bbb";
		var input3 = "ccc";
		var counter1 = new AtomicInteger(0);
		var counter2 = new AtomicInteger(0);
		var counter3 = new AtomicInteger(0);
		var sampler = new EveryNthSampler<String>(3, 2);
		// Put at max capacity 2
		incrementCounterIfSampled(counter1, sampler, input1);
		incrementCounterIfSampled(counter2, sampler, input2);
		// Force map clear by going over capacity w/ 3rd entry
		incrementCounterIfSampled(counter3, sampler, input3);
		assertThat(counter3).hasValue(1);
		// Wait for the map to clear async and be sure the new counter remains
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.untilAsserted(() -> assertThat(sampler)
				.extracting("inputCounters", InstanceOfAssertFactories.map(String.class, AtomicLong.class))
				.containsOnlyKeys(input3));
		// The side effect is the cleared entries will fire again as their counter is
		// effectively reset
		incrementCounterIfSampled(counter1, sampler, input1);
		assertThat(counter1).hasValue(2);
	}

	@ParameterizedTest
	@ValueSource(longs = { 0, -1 })
	void frequencyMustBePositive(long frequency) {
		assertThatIllegalStateException().isThrownBy(() -> new EveryNthSampler<>(frequency, 500))
			.withMessage("nth must be a positive value");
	}

	@ParameterizedTest
	@ValueSource(longs = { 0, -1 })
	void maxInputsMustBePositive(long maxInput) {
		assertThatIllegalStateException().isThrownBy(() -> new EveryNthSampler<>(100, maxInput))
			.withMessage("maxInputs must be a positive value");
	}

	private <T> void incrementCounterIfSampled(AtomicInteger counter, EveryNthSampler<T> sampler, T input) {
		if (sampler.trySample(input)) {
			counter.incrementAndGet();
		}
	}

}
