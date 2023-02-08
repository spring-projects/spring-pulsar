/*
 * Copyright 2023 the original author or authors.
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

package org.springframework.pulsar.spring.cloud.stream.binder;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.Spy;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.core.PulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.spring.cloud.stream.binder.properties.PulsarBinderConfigurationProperties;
import org.springframework.pulsar.spring.cloud.stream.binder.properties.PulsarConsumerProperties;
import org.springframework.pulsar.spring.cloud.stream.binder.properties.PulsarProducerProperties;
import org.springframework.pulsar.spring.cloud.stream.binder.provisioning.PulsarTopicProvisioner;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.util.Assert;
import org.springframework.util.MimeTypeUtils;

/**
 * Tests for {@link PulsarMessageChannelBinder}.
 *
 * @author Soby Chacko
 */
public class PulsarBinderTests extends
		PartitionCapableBinderTests<AbstractPulsarTestBinder, ExtendedConsumerProperties<PulsarConsumerProperties>, ExtendedProducerProperties<PulsarProducerProperties>>
		implements PulsarTestContainerSupport {

	private PulsarTestBinder binder;

	@Nullable
	protected PulsarClient pulsarClient;

	@BeforeEach
	void createPulsarClient() throws PulsarClientException {
		pulsarClient = PulsarClient.builder().serviceUrl(PulsarTestContainerSupport.getPulsarBrokerUrl()).build();
	}

	@AfterEach
	void closePulsarClient() throws PulsarClientException {
		if (pulsarClient != null && !pulsarClient.isClosed()) {
			pulsarClient.close();
		}
	}

	private final String CLASS_UNDER_TEST_NAME = PulsarMessageChannelBinder.class.getSimpleName();

	@Override
	protected boolean usesExplicitRouting() {
		return false;
	}

	@Override
	protected String getClassUnderTestName() {
		return CLASS_UNDER_TEST_NAME;
	}

	@Override
	protected PulsarTestBinder getBinder() {
		PulsarAdministration pulsarAdministration = new PulsarAdministration(
				Map.of("serviceUrl", PulsarTestContainerSupport.getHttpServiceUrl()));
		PulsarBinderConfigurationProperties configurationProperties = new PulsarBinderConfigurationProperties();
		PulsarTopicProvisioner pulsarTopicProvisioner = new PulsarTopicProvisioner(pulsarAdministration,
				configurationProperties);

		PulsarProducerFactory<?> producerFactory = new DefaultPulsarProducerFactory<>(pulsarClient,
				Collections.emptyMap());
		PulsarTemplate<?> pulsarTemplate = new PulsarTemplate<>(producerFactory);

		Map<String, Object> config = Map.of("subscriptionInitialPosition", SubscriptionInitialPosition.Earliest);
		DefaultPulsarConsumerFactory<?> consumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient, config);

		if (this.binder == null) {
			this.binder = new PulsarTestBinder(pulsarTopicProvisioner, pulsarTemplate, consumerFactory,
					new DefaultSchemaResolver());
		}
		return this.binder;
	}

	@Override
	protected ExtendedConsumerProperties<PulsarConsumerProperties> createConsumerProperties() {
		final ExtendedConsumerProperties<PulsarConsumerProperties> pulsarConsumerProperties = new ExtendedConsumerProperties<>(
				new PulsarConsumerProperties());
		return pulsarConsumerProperties;
	}

	@Override
	public Spy spyOn(String name) {
		return null;
	}

	private ExtendedProducerProperties<PulsarProducerProperties> createProducerProperties() {
		return this.createProducerProperties(null);
	}

	@Override
	protected ExtendedProducerProperties<PulsarProducerProperties> createProducerProperties(TestInfo testInto) {
		return new ExtendedProducerProperties<>(new PulsarProducerProperties());
	}

	@Override
	protected void binderBindUnbindLatency() throws InterruptedException {
		Thread.sleep(500);
	}

	@Test
	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testSendAndReceive(TestInfo testInfo) throws Exception {
		Binder binder = getBinder();
		BindingProperties outputBindingProperties = createProducerBindingProperties(createProducerProperties());

		DirectChannel moduleOutputChannel = createBindableChannel("output", outputBindingProperties);
		ExtendedConsumerProperties<PulsarConsumerProperties> consumerProperties = createConsumerProperties();
		DirectChannel moduleInputChannel = createBindableChannel("input",
				createConsumerBindingProperties(consumerProperties));

		Binding<MessageChannel> producerBinding = binder.bindProducer("foo.bar", moduleOutputChannel,
				outputBindingProperties.getProducer());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer("foo.bar", null, moduleInputChannel,
				consumerProperties);

		Message<?> message = org.springframework.integration.support.MessageBuilder
				.withPayload("foo".getBytes(StandardCharsets.UTF_8)).build();

		// Let the consumer actually bind to the producer before sending a msg
		binderBindUnbindLatency();
		moduleOutputChannel.send(message);
		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<Message<byte[]>> inboundMessageRef = new AtomicReference<>();
		moduleInputChannel.subscribe(message1 -> {
			try {
				inboundMessageRef.set((Message<byte[]>) message1);
			}
			finally {
				latch.countDown();
			}
		});
		Assert.isTrue(latch.await(5, TimeUnit.SECONDS), "Failed to receive message");

		assertThat(inboundMessageRef.get()).isNotNull();
		assertThat(new String(inboundMessageRef.get().getPayload(), StandardCharsets.UTF_8)).isEqualTo("foo");

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testAnonymousGroup(TestInfo testInfo) throws Exception {
		Binder binder = getBinder();
		BindingProperties producerBindingProperties = createProducerBindingProperties(
				createProducerProperties(testInfo));
		DirectChannel output = createBindableChannel("output", producerBindingProperties);
		Binding<MessageChannel> producerBinding = binder.bindProducer(
				String.format("defaultGroup%s0", getDestinationNameDelimiter()), output,
				producerBindingProperties.getProducer());

		QueueChannel input1 = new QueueChannel();
		Binding<MessageChannel> binding1 = binder.bindConsumer(
				String.format("defaultGroup%s0", getDestinationNameDelimiter()), null, input1,
				createConsumerProperties());

		QueueChannel input2 = new QueueChannel();
		Binding<MessageChannel> binding2 = binder.bindConsumer(
				String.format("defaultGroup%s0", getDestinationNameDelimiter()), null, input2,
				createConsumerProperties());

		String testPayload1 = "foo-" + UUID.randomUUID().toString();
		output.send(MessageBuilder.withPayload(testPayload1)
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN).build());

		Message<byte[]> receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String(receivedMessage1.getPayload())).isEqualTo(testPayload1);

		Message<byte[]> receivedMessage2 = (Message<byte[]>) receive(input2);
		assertThat(receivedMessage2).isNotNull();
		assertThat(new String(receivedMessage2.getPayload())).isEqualTo(testPayload1);

		binding2.unbind();

		String testPayload2 = "foo-" + UUID.randomUUID().toString();
		output.send(MessageBuilder.withPayload(testPayload2)
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN).build());

		binding2 = binder.bindConsumer(String.format("defaultGroup%s0", getDestinationNameDelimiter()), null, input2,
				createConsumerProperties());
		String testPayload3 = "foo-" + UUID.randomUUID().toString();
		output.send(MessageBuilder.withPayload(testPayload3)
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN).build());

		receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String(receivedMessage1.getPayload())).isEqualTo(testPayload2);
		receivedMessage1 = (Message<byte[]>) receive(input1);
		assertThat(receivedMessage1).isNotNull();
		assertThat(new String(receivedMessage1.getPayload())).isEqualTo(testPayload3);

		receivedMessage2 = (Message<byte[]>) receive(input2);
		assertThat(receivedMessage2).isNotNull();
		assertThat(new String(receivedMessage2.getPayload())).isEqualTo(testPayload1);

		producerBinding.unbind();
		binding1.unbind();
		binding2.unbind();
	}

	@Test
	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testSendAndReceiveNoOriginalContentType(TestInfo testInfo) throws Exception {
		Binder binder = getBinder();

		BindingProperties producerBindingProperties = createProducerBindingProperties(
				createProducerProperties(testInfo));
		DirectChannel moduleOutputChannel = createBindableChannel("output", producerBindingProperties);
		BindingProperties inputBindingProperties = createConsumerBindingProperties(createConsumerProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", inputBindingProperties);
		Binding<MessageChannel> producerBinding = binder.bindProducer(
				String.format("bar%s0", getDestinationNameDelimiter()), moduleOutputChannel,
				producerBindingProperties.getProducer());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				String.format("bar%s0", getDestinationNameDelimiter()), "testSendAndReceiveNoOriginalContentType",
				moduleInputChannel, createConsumerProperties());
		binderBindUnbindLatency();

		Message<?> message = MessageBuilder.withPayload("foo")
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN).build();
		moduleOutputChannel.send(message);
		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<Message<byte[]>> inboundMessageRef = new AtomicReference<Message<byte[]>>();
		moduleInputChannel.subscribe(message1 -> {
			try {
				inboundMessageRef.set((Message<byte[]>) message1);
			}
			finally {
				latch.countDown();
			}
		});

		moduleOutputChannel.send(message);
		Assert.isTrue(latch.await(5, TimeUnit.SECONDS), "Failed to receive message");
		assertThat(inboundMessageRef.get()).isNotNull();
		assertThat(inboundMessageRef.get().getPayload()).isEqualTo("foo".getBytes());
		// TODO: The below content-type should be TEXT_PLAIN, but default to
		// application/json
		// This is because we don't currently preserve any message headers on send. We
		// should look into this soon.
		// Also, the content-type is "application/json" (with double quotes). We will have
		// to fix that as well.
		assertThat(inboundMessageRef.get().getHeaders().get(MessageHeaders.CONTENT_TYPE).toString())
				.contains(MimeTypeUtils.APPLICATION_JSON.toString());
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@Override
	@Disabled
	public void testPartitionedModuleSpEL(TestInfo testInfo) throws Exception {
		// This use-case needs to be further evaluated for Pulsar binder.
	}

}
