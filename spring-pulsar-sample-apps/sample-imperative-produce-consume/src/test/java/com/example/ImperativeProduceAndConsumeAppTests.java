/*
 * Copyright 2012-2023 the original author or authors.
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

package com.example;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.pulsar.test.model.UserRecord;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import com.example.ImperativeProduceAndConsumeApp.Bar;
import com.example.ImperativeProduceAndConsumeApp.Foo;

@SpringBootTest
@ExtendWith(OutputCaptureExtension.class)
class ImperativeProduceAndConsumeAppTests implements PulsarTestContainerSupport {

	@DynamicPropertySource
	static void pulsarProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.pulsar.client.service-url", PULSAR_CONTAINER::getPulsarBrokerUrl);
		registry.add("spring.pulsar.admin.service-url", PULSAR_CONTAINER::getHttpServiceUrl);
	}

	@Test
	void produceConsumeWithPrimitiveMessageType(CapturedOutput output) {
		verifyProduceConsume(output, 10, (i) -> "ProduceConsumeWithPrimitiveMessageType:" + i);
	}

	@Test
	void produceConsumeWithComplexMessageType(CapturedOutput output) {
		verifyProduceConsume(output,10,
				(i) -> new Foo("ProduceConsumeWithComplexMessageType", i));
	}

	@Test
	void produceConsumeWithPartitions(CapturedOutput output) {
		verifyProduceConsume(output, 10, (i) -> "ProduceConsumeWithPartitions:" + i);
	}

	@Test
	void produceConsumeBatchListener(CapturedOutput output) {
		verifyProduceConsume(output, 100, (i) -> new Foo("ProduceConsumeBatchListener", i));
	}

	@Test
	void produceConsumeDefaultMappings(CapturedOutput output) {
		verifyProduceConsume(output, 10, (i) -> new Bar("ProduceConsumeDefaultMappings:" + i));
	}

	@Test
	void produceConsumeCustomObjectMapper(CapturedOutput output) {
		// base age is 30 then ser adds 10 then deser adds 5
		var expectedAge = 30 + 10 + 5;
		verifyProduceConsume(output, 10,
				(i) -> new UserRecord("user-%d".formatted(i), 30),
				(i) -> new UserRecord("user-%d-ser-deser".formatted(i), expectedAge));
	}

	private void verifyProduceConsume(CapturedOutput output, int numExpectedMessages,
			Function<Integer, Object> expectedMessageFactory) {
		this.verifyProduceConsume(output, numExpectedMessages, expectedMessageFactory, expectedMessageFactory);
	}

	private void verifyProduceConsume(CapturedOutput output, int numExpectedMessages,
			Function<Integer, Object> expectedProducedMessageFactory,
			Function<Integer, Object> expectedConsumedMessageFactory) {
		var expectedOutput = new ArrayList<String>();
		IntStream.range(0, numExpectedMessages).forEachOrdered((i) -> {
			var expectedProducedMsg = expectedProducedMessageFactory.apply(i);
			var expectedConsumedMsg = expectedConsumedMessageFactory.apply(i);
			expectedOutput.add("++++++PRODUCE %s------".formatted(expectedProducedMsg));
			expectedOutput.add("++++++CONSUME %s------".formatted(expectedConsumedMsg));
		});
		Awaitility.waitAtMost(Duration.ofSeconds(15))
				.untilAsserted(() -> assertThat(output).contains(expectedOutput));
	}
}
