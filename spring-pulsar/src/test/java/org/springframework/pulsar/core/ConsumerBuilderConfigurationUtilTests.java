/*
 * Copyright 2022 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MultiplierRedeliveryBackoff;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.lang.Nullable;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Unit tests for {@link ConsumerBuilderConfigurationUtil}.
 *
 * @author Chris Bono
 */
public class ConsumerBuilderConfigurationUtilTests {

	private ConsumerBuilder<String> builder;

	@BeforeEach
	void prepareBuilder() throws PulsarClientException {
		PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
		builder = pulsarClient.newConsumer(Schema.STRING);
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("loadConfTestProvider")
	void loadConfTest(String testName, String propName, @Nullable Object objOnBuilder, @Nullable Object objOnProps,
			@Nullable Object expectedObj) {

		if (objOnBuilder != null) {
			ReflectionTestUtils.invokeSetterMethod(builder, propName, objOnBuilder);
		}

		Map<String, Object> props = new HashMap<>();
		if (objOnProps != null) {
			props.put(propName, objOnProps);
		}
		Map<String, Object> propsBeforeUtil = new HashMap<>(props);

		ConsumerBuilderConfigurationUtil.loadConf(builder, props);

		assertThat(this.builder).extracting("conf")
				.asInstanceOf(InstanceOfAssertFactories.type(ConsumerConfigurationData.class)).extracting(propName)
				.isEqualTo(expectedObj);

		assertThat(props).isEqualTo(propsBeforeUtil);
	}

	private static Stream<Arguments> loadConfTestProvider() {

		DeadLetterPolicy deadLetterPolicyOnBuilder = DeadLetterPolicy.builder().deadLetterTopic("dlt-topic")
				.maxRedeliverCount(1).build();
		DeadLetterPolicy deadLetterPolicyInProps = DeadLetterPolicy.builder().deadLetterTopic("dlt-topic")
				.maxRedeliverCount(2).build();

		RedeliveryBackoff nackRedeliveryBackoffOnBuilder = MultiplierRedeliveryBackoff.builder().minDelayMs(1000)
				.maxDelayMs(5000).build();
		RedeliveryBackoff nackRedeliveryBackoffInProps = MultiplierRedeliveryBackoff.builder().minDelayMs(2000)
				.maxDelayMs(4000).build();

		RedeliveryBackoff ackRedeliveryBackoffOnBuilder = MultiplierRedeliveryBackoff.builder().minDelayMs(1000)
				.maxDelayMs(5000).build();
		RedeliveryBackoff ackRedeliveryBackoffInProps = MultiplierRedeliveryBackoff.builder().minDelayMs(2000)
				.maxDelayMs(4000).build();

		return Stream.of(arguments("loadConfNoDeadLetterPolicy", "deadLetterPolicy", null, null, null),
				arguments("loadConfDeadLetterPolicyOnBuilder", "deadLetterPolicy", deadLetterPolicyOnBuilder, null,
						deadLetterPolicyOnBuilder),
				arguments("loadConfDeadLetterPolicyInProps", "deadLetterPolicy", null, deadLetterPolicyInProps,
						deadLetterPolicyInProps),
				arguments("loadConfDeadLetterPolicyOnBuilderAndInProps", "deadLetterPolicy", deadLetterPolicyOnBuilder,
						deadLetterPolicyInProps, deadLetterPolicyInProps),
				arguments("loadConfNoNegativeAckRedeliveryBackoff", "negativeAckRedeliveryBackoff", null, null, null),
				arguments("loadConfNegativeAckRedeliveryBackoffOnBuilder", "negativeAckRedeliveryBackoff",
						nackRedeliveryBackoffOnBuilder, null, nackRedeliveryBackoffOnBuilder),
				arguments("loadConfNegativeAckRedeliveryBackoffInProps", "negativeAckRedeliveryBackoff", null,
						nackRedeliveryBackoffInProps, nackRedeliveryBackoffInProps),
				arguments("loadConfNegativeAckRedeliveryBackoffOnBuilderAndInProps", "negativeAckRedeliveryBackoff",
						nackRedeliveryBackoffOnBuilder, nackRedeliveryBackoffInProps, nackRedeliveryBackoffInProps),
				arguments("loadConfNoAckRedeliveryBackoff", "ackTimeoutRedeliveryBackoff", null, null, null),
				arguments("loadConfAckRedeliveryBackoffOnBuilder", "ackTimeoutRedeliveryBackoff",
						ackRedeliveryBackoffOnBuilder, null, ackRedeliveryBackoffOnBuilder),
				arguments("loadConfAckRedeliveryBackoffInProps", "ackTimeoutRedeliveryBackoff", null,
						ackRedeliveryBackoffInProps, ackRedeliveryBackoffInProps),
				arguments("loadConfAckRedeliveryBackoffOnBuilderAndInProps", "ackTimeoutRedeliveryBackoff",
						ackRedeliveryBackoffOnBuilder, ackRedeliveryBackoffInProps, ackRedeliveryBackoffInProps));
	}

}
