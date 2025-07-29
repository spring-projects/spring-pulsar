/*
 * Copyright 2012-present the original author or authors.
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.stream.IntStream;

import com.example.SpringPulsarBinderSampleApp.Bar;
import com.example.SpringPulsarBinderSampleApp.Foo;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ExtendWith(OutputCaptureExtension.class)
class SpringPulsarBinderSampleAppTests implements PulsarTestContainerSupport {

	@DynamicPropertySource
	static void pulsarProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.pulsar.client.service-url", PULSAR_CONTAINER::getPulsarBrokerUrl);
		registry.add("spring.pulsar.admin.service-url", PULSAR_CONTAINER::getHttpServiceUrl);
	}

	@Test
	void produceConsumeWithPrimitiveMessageType(CapturedOutput output) {
		var expectedOutput = new ArrayList<String>();
		IntStream.range(0, 10).forEachOrdered((i) -> {
			var foo = new Foo("fooSupplier:" + i);
			var bar = new Bar(foo);
			expectedOutput.add("++++++SOURCE %s------".formatted(foo));
			expectedOutput.add("++++++PROCESSOR %s --> %s------".formatted(foo, bar));
			expectedOutput.add("++++++SINK %s------".formatted(bar));
		});
		Awaitility.waitAtMost(Duration.ofSeconds(15))
				.untilAsserted(() -> assertThat(output).contains(expectedOutput));
	}

}
