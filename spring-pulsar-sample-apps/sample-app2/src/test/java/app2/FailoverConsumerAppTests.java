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

package app2;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

@SpringBootTest
@ExtendWith(OutputCaptureExtension.class)
class FailoverConsumerAppTests implements PulsarTestContainerSupport {

	@DynamicPropertySource
	static void pulsarProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.pulsar.client.service-url", PULSAR_CONTAINER::getPulsarBrokerUrl);
		registry.add("spring.pulsar.admin.service-url", PULSAR_CONTAINER::getHttpServiceUrl);
	}

	@Test
	void produceConsumeWithPrimitiveMessageType(CapturedOutput output) {
		List <String> expectedOutput = new ArrayList<>();
		IntStream.range(0, 10).forEachOrdered((i) -> {
			expectedOutput.add("++++++PRODUCE_0 hello_0------");
			expectedOutput.add("++++++PRODUCE_1 hello_1------");
			expectedOutput.add("++++++PRODUCE_2 hello_2------");
		});
		Awaitility.waitAtMost(Duration.ofSeconds(15))
				.untilAsserted(() -> assertThat(output).satisfies((out) -> {
					assertThat(output).contains(expectedOutput);
					assertListenerConsumedNumMessagesFromSinglePartitionOnly(0, out.toString());
					assertListenerConsumedNumMessagesFromSinglePartitionOnly(1, out.toString());
					assertListenerConsumedNumMessagesFromSinglePartitionOnly(2, out.toString());
				}));
	}

	private void assertListenerConsumedNumMessagesFromSinglePartitionOnly(int listenerIndex, String output) {
		var msgsConsumedByPartition = new HashMap<>();
		msgsConsumedByPartition.put(0, numMessagesConsumed(listenerIndex, 0, output));
		msgsConsumedByPartition.put(1, numMessagesConsumed(listenerIndex, 1, output));
		msgsConsumedByPartition.put(2, numMessagesConsumed(listenerIndex, 2, output));
		var numMatched = msgsConsumedByPartition.values().stream().filter(Long.valueOf(10)::equals).count();
		assertThat(numMatched).isEqualTo(1);
	}

	private long numMessagesConsumed(int consumerPartition, int producerPartition, String output) {
		var regex = "(\\+\\+\\+\\+\\+\\+CONSUME_%d hello_%d------)".formatted(consumerPartition, producerPartition);
		return Pattern.compile(regex).matcher(output).results().count();
	}
}
