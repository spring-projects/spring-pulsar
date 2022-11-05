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
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.MessageCrypto;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.MessagePayloadProcessor;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.lang.Nullable;

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
	void loadConfTest(String testName, String propName, @Nullable ConsumerBuilderCustomizer<String> objOnBuilder,
			@Nullable Object objOnProps, @Nullable Object expectedObj) {

		if (objOnBuilder != null) {
			objOnBuilder.customize(builder);
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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Stream<Arguments> loadConfTestProvider() {
		DeadLetterPolicy deadLetterPolicyOnBuilder = DeadLetterPolicy.builder().deadLetterTopic("dlt-topic")
				.maxRedeliverCount(1).build();
		DeadLetterPolicy deadLetterPolicyInProps = DeadLetterPolicy.builder().deadLetterTopic("dlt-topic")
				.maxRedeliverCount(2).build();
		ConsumerBuilderCustomizer<String> deadLetterPolicyCustomizer = c -> c
				.deadLetterPolicy(deadLetterPolicyOnBuilder);

		MessageListener<String> messageListenerOnBuilder = mock(MessageListener.class);
		MessageListener<String> messageListenerInProps = mock(MessageListener.class);
		ConsumerBuilderCustomizer<String> messageListenerCustomizer = c -> c.messageListener(messageListenerOnBuilder);

		ConsumerEventListener consumerEventListenerOnBuilder = mock(ConsumerEventListener.class);
		ConsumerEventListener consumerEventListenerInProps = mock(ConsumerEventListener.class);
		ConsumerBuilderCustomizer<String> consumerEventListenerCustomizer = c -> c
				.consumerEventListener(consumerEventListenerOnBuilder);

		RedeliveryBackoff nackRedeliveryBackoffOnBuilder = mock(RedeliveryBackoff.class);
		RedeliveryBackoff nackRedeliveryBackoffInProps = mock(RedeliveryBackoff.class);
		ConsumerBuilderCustomizer<String> nackRedeliveryBackoffCustomizer = c -> c
				.negativeAckRedeliveryBackoff(nackRedeliveryBackoffOnBuilder);

		RedeliveryBackoff ackRedeliveryBackoffOnBuilder = mock(RedeliveryBackoff.class);
		RedeliveryBackoff ackRedeliveryBackoffInProps = mock(RedeliveryBackoff.class);
		ConsumerBuilderCustomizer<String> ackRedeliveryBackoffCustomizer = c -> c
				.ackTimeoutRedeliveryBackoff(ackRedeliveryBackoffOnBuilder);

		CryptoKeyReader cryptoKeyReaderOnBuilder = mock(CryptoKeyReader.class);
		CryptoKeyReader cryptoKeyReaderInProps = mock(CryptoKeyReader.class);
		ConsumerBuilderCustomizer<String> cryptoKeyReaderCustomizer = c -> c.cryptoKeyReader(cryptoKeyReaderOnBuilder);

		MessageCrypto messageCryptoOnBuilder = mock(MessageCrypto.class);
		MessageCrypto messageCryptoInProps = mock(MessageCrypto.class);
		ConsumerBuilderCustomizer<String> messageCryptoCustomizer = c -> c.messageCrypto(messageCryptoOnBuilder);

		BatchReceivePolicy batchReceivePolicyOnBuilder = mock(BatchReceivePolicy.class);
		BatchReceivePolicy batchReceivePolicyInProps = mock(BatchReceivePolicy.class);
		ConsumerBuilderCustomizer<String> batchReceivePolicyCustomizer = c -> c
				.batchReceivePolicy(batchReceivePolicyOnBuilder);

		MessagePayloadProcessor payloadProcessorOnBuilder = mock(MessagePayloadProcessor.class);
		MessagePayloadProcessor payloadProcessorInProps = mock(MessagePayloadProcessor.class);
		ConsumerBuilderCustomizer<String> payloadProcessorCustomizer = c -> c
				.messagePayloadProcessor(payloadProcessorOnBuilder);

		return Stream.of(arguments("loadConfNoDeadLetterPolicy", "deadLetterPolicy", null, null, null),
				arguments("loadConfDeadLetterPolicyOnBuilder", "deadLetterPolicy", deadLetterPolicyCustomizer, null,
						deadLetterPolicyOnBuilder),
				arguments("loadConfDeadLetterPolicyInProps", "deadLetterPolicy", null, deadLetterPolicyInProps,
						deadLetterPolicyInProps),
				arguments("loadConfDeadLetterPolicyOnBuilderAndInProps", "deadLetterPolicy", deadLetterPolicyCustomizer,
						deadLetterPolicyInProps, deadLetterPolicyInProps),

				arguments("loadConfNoMessageListener", "messageListener", null, null, null),
				arguments("loadConfMessageListenerOnBuilder", "messageListener", messageListenerCustomizer, null,
						messageListenerOnBuilder),
				arguments("loadConfMessageListenerInProps", "messageListener", null, messageListenerInProps,
						messageListenerInProps),
				arguments("loadConfMessageListenerOnBuilderAndInProps", "messageListener", messageListenerCustomizer,
						messageListenerInProps, messageListenerInProps),
				arguments("loadConfNoConsumerEventListener", "consumerEventListener", null, null, null),
				arguments("loadConfConsumerEventListenerOnBuilder", "consumerEventListener",
						consumerEventListenerCustomizer, null, consumerEventListenerOnBuilder),
				arguments("loadConfConsumerEventListenerInProps", "consumerEventListener", null,
						consumerEventListenerInProps, consumerEventListenerInProps),
				arguments("loadConfConsumerEventListenerOnBuilderAndInProps", "consumerEventListener",
						consumerEventListenerCustomizer, consumerEventListenerInProps, consumerEventListenerInProps),
				arguments("loadConfNoNegativeAckRedeliveryBackoff", "negativeAckRedeliveryBackoff", null, null, null),
				arguments("loadConfNegativeAckRedeliveryBackoffOnBuilder", "negativeAckRedeliveryBackoff",
						nackRedeliveryBackoffCustomizer, null, nackRedeliveryBackoffOnBuilder),
				arguments("loadConfNegativeAckRedeliveryBackoffInProps", "negativeAckRedeliveryBackoff", null,
						nackRedeliveryBackoffInProps, nackRedeliveryBackoffInProps),
				arguments("loadConfNegativeAckRedeliveryBackoffOnBuilderAndInProps", "negativeAckRedeliveryBackoff",
						nackRedeliveryBackoffCustomizer, nackRedeliveryBackoffInProps, nackRedeliveryBackoffInProps),
				arguments("loadConfNoAckRedeliveryBackoff", "ackTimeoutRedeliveryBackoff", null, null, null),
				arguments("loadConfAckRedeliveryBackoffOnBuilder", "ackTimeoutRedeliveryBackoff",
						ackRedeliveryBackoffCustomizer, null, ackRedeliveryBackoffOnBuilder),
				arguments("loadConfAckRedeliveryBackoffInProps", "ackTimeoutRedeliveryBackoff", null,
						ackRedeliveryBackoffInProps, ackRedeliveryBackoffInProps),
				arguments("loadConfAckRedeliveryBackoffOnBuilderAndInProps", "ackTimeoutRedeliveryBackoff",
						ackRedeliveryBackoffCustomizer, ackRedeliveryBackoffInProps, ackRedeliveryBackoffInProps),
				arguments("loadConfNoCryptoKeyReader", "cryptoKeyReader", null, null, null),
				arguments("loadConfCryptoKeyReaderOnBuilder", "cryptoKeyReader", cryptoKeyReaderCustomizer, null,
						cryptoKeyReaderOnBuilder),
				arguments("loadConfCryptoKeyReaderInProps", "cryptoKeyReader", null, cryptoKeyReaderInProps,
						cryptoKeyReaderInProps),
				arguments("loadConfCryptoKeyReaderOnBuilderAndInProps", "cryptoKeyReader", cryptoKeyReaderCustomizer,
						cryptoKeyReaderInProps, cryptoKeyReaderInProps),
				arguments("loadConfNoMessageCrypto", "messageCrypto", null, null, null),
				arguments("loadConfMessageCryptoOnBuilder", "messageCrypto", messageCryptoCustomizer, null,
						messageCryptoOnBuilder),
				arguments("loadConfMessageCryptoInProps", "messageCrypto", null, messageCryptoInProps,
						messageCryptoInProps),
				arguments("loadConfMessageCryptoOnBuilderAndInProps", "messageCrypto", messageCryptoCustomizer,
						messageCryptoInProps, messageCryptoInProps),
				arguments("loadConfNoBatchReceivePolicy", "batchReceivePolicy", null, null, null),
				arguments("loadConfBatchReceivePolicyOnBuilder", "batchReceivePolicy", batchReceivePolicyCustomizer,
						null, batchReceivePolicyOnBuilder),
				arguments("loadConfBatchReceivePolicyInProps", "batchReceivePolicy", null, batchReceivePolicyInProps,
						batchReceivePolicyInProps),
				arguments("loadConfBatchReceivePolicyOnBuilderAndInProps", "batchReceivePolicy",
						batchReceivePolicyCustomizer, batchReceivePolicyInProps, batchReceivePolicyInProps),
				arguments("loadConfNoPayloadProcessor", "payloadProcessor", null, null, null),
				arguments("loadConfPayloadProcessorOnBuilder", "payloadProcessor", payloadProcessorCustomizer, null,
						payloadProcessorOnBuilder),
				arguments("loadConfPayloadProcessorInProps", "payloadProcessor", null, payloadProcessorInProps,
						payloadProcessorInProps),
				arguments("loadConfPayloadProcessorOnBuilderAndInProps", "payloadProcessor", payloadProcessorCustomizer,
						payloadProcessorInProps, payloadProcessorInProps));
	}

}
