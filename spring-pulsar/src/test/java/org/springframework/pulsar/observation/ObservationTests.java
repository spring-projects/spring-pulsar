/*
 * Copyright 2022-2023 the original author or authors.
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

package org.springframework.pulsar.observation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.config.ConcurrentPulsarListenerContainerFactory;
import org.springframework.pulsar.config.PulsarListenerContainerFactory;
import org.springframework.pulsar.core.DefaultPulsarClientFactory;
import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.DefaultTopicResolver;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.core.PulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.listener.PulsarContainerProperties;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import io.micrometer.common.KeyValues;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.core.tck.MeterRegistryAssert;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingSenderTracingObservationHandler;
import io.micrometer.tracing.propagation.Propagator;
import io.micrometer.tracing.test.simple.SimpleSpan;
import io.micrometer.tracing.test.simple.SimpleTracer;

/**
 * Tests for {@link PulsarTemplateObservation send} and {@link PulsarListenerObservation
 * receive} observations in Spring Pulsar.
 * <p>
 * Differs from {@link ObservationIntegrationTests} in that it uses a test observation
 * registry and verifies more details such as propagation and message content.
 *
 * @author Chris Bono
 */
@SpringJUnitConfig
@DirtiesContext
public class ObservationTests implements PulsarTestContainerSupport {

	private static final String LISTENER_ID_TAG = "spring.pulsar.listener.id";

	private static final String OBS1_ID = "obs1-id-0";

	private static final String OBS2_ID = "obs2-id-0";

	private static final String RECEIVER_EXTRA_TAG = "receiver-extra-tag-bean-name";

	private static final String SENDER_EXTRA_TAG = "sender-extra-tag-bean-name";

	private static final String TAG1 = "tag1";

	private static final String TAG1_VALUE = "tag1-value";

	private static final String TAG2 = "tag2";

	private static final String TAG2_VALUE = "tag2-value";

	private static final String TEMPLATE_NAME_TAG = "spring.pulsar.template.name";

	private static final String TEMPLATE_NAME = "observationTestsTemplate";

	@Test
	void sendAndReceiveCreatesExpectedSpansAndMetrics(@Autowired ObservationTestAppListeners appListeners,
			@Autowired PulsarTemplate<String> template, @Autowired SimpleTracer tracer,
			@Autowired MeterRegistry meterRegistry) throws Exception {

		template.send("obs1-topic", "hello");

		// The final message should have tags propagated through
		assertThat(appListeners.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(appListeners.message).isNotNull();
		assertThat(appListeners.message.getProperty(TAG1)).isEqualTo(TAG1_VALUE);
		assertThat(appListeners.message.getProperty(TAG2)).isEqualTo(TAG2_VALUE);

		Deque<SimpleSpan> spans = tracer.getSpans();
		assertThat(spans).hasSize(4);

		// Span1: The initial template.send to obs1-topic
		SimpleSpan span = spans.pollFirst();
		assertThat(span.getName()).isEqualTo("obs1-topic send");
		assertThat(span.getTags()).containsEntry(TEMPLATE_NAME_TAG, TEMPLATE_NAME);
		assertThat(span.getTags()).containsEntry(SENDER_EXTRA_TAG, TEMPLATE_NAME);

		// Span2: The listen1.receive from obs1-topic
		await().until(() -> spans.peekFirst().getTags().size() == 4);
		span = spans.pollFirst();
		assertThat(span.getName()).isEqualTo("persistent://public/default/obs1-topic receive");
		assertThat(span.getTags()).containsEntry(LISTENER_ID_TAG, OBS1_ID);
		assertThat(span.getTags()).containsEntry(RECEIVER_EXTRA_TAG, OBS1_ID);
		assertThat(span.getTags()).containsEntry(TAG1, TAG1_VALUE);
		assertThat(span.getTags()).containsEntry(TAG2, TAG2_VALUE);

		// Span3: The template.send from listen1 to obs2-topic
		await().until(() -> spans.peekFirst().getTags().size() == 2);
		span = spans.pollFirst();
		assertThat(span.getName()).isEqualTo("obs2-topic send");
		assertThat(span.getTags()).containsEntry(TEMPLATE_NAME_TAG, TEMPLATE_NAME);
		assertThat(span.getTags()).containsEntry(SENDER_EXTRA_TAG, TEMPLATE_NAME);

		// Span4: The final listen2.receive from obs2-topic - does not get extra tag
		// (hence count = 3)
		await().until(() -> spans.peekFirst().getTags().size() == 3);
		span = spans.pollFirst();
		assertThat(span.getName()).isEqualTo("persistent://public/default/obs2-topic receive");
		assertThat(span.getTags()).containsEntry(LISTENER_ID_TAG, OBS2_ID);
		assertThat(span.getTags()).containsEntry(TAG1, TAG1_VALUE);
		assertThat(span.getTags()).containsEntry(TAG2, TAG2_VALUE);

		MeterRegistryAssert.assertThat(meterRegistry)
				.hasTimerWithNameAndTags("spring.pulsar.listener",
						KeyValues.of(LISTENER_ID_TAG, OBS1_ID, RECEIVER_EXTRA_TAG, OBS1_ID))
				.hasTimerWithNameAndTags("spring.pulsar.listener", KeyValues.of(LISTENER_ID_TAG, OBS2_ID));
		assertThat(meterRegistry.find("spring.pulsar.template")
				.tags(TEMPLATE_NAME_TAG, TEMPLATE_NAME, SENDER_EXTRA_TAG, TEMPLATE_NAME).timer()).isNotNull()
						.extracting(Timer::count).isEqualTo(2L);
	}

	@Configuration(proxyBeanMethods = false)
	@EnablePulsar
	static class ObservationTestAppConfig {

		@Bean
		PulsarProducerFactory<String> pulsarProducerFactory(PulsarClient pulsarClient) {
			return new DefaultPulsarProducerFactory<>(pulsarClient);
		}

		@Bean
		public PulsarClient pulsarClient() throws PulsarClientException {
			return new DefaultPulsarClientFactory(PulsarTestContainerSupport.getPulsarBrokerUrl()).createClient();
		}

		@Bean(name = "observationTestsTemplate")
		PulsarTemplate<String> pulsarTemplate(PulsarProducerFactory<String> pulsarProducerFactory,
				ObservationRegistry observationRegistry) {
			return new PulsarTemplate<>(pulsarProducerFactory, null, new DefaultSchemaResolver(),
					new DefaultTopicResolver(), true);
		}

		@Bean
		PulsarTemplateObservationConvention pulsarTemplateConvention() {
			return new DefaultPulsarTemplateObservationConvention() {
				@Override
				public KeyValues getLowCardinalityKeyValues(PulsarMessageSenderContext context) {
					return super.getLowCardinalityKeyValues(context).and(SENDER_EXTRA_TAG, context.getBeanName());
				}
			};
		}

		@Bean
		PulsarConsumerFactory<?> pulsarConsumerFactory(PulsarClient pulsarClient) {
			return new DefaultPulsarConsumerFactory<>(pulsarClient, null);
		}

		@Bean
		PulsarListenerContainerFactory pulsarListenerContainerFactory(
				PulsarConsumerFactory<Object> pulsarConsumerFactory) {
			PulsarContainerProperties containerProperties = new PulsarContainerProperties();
			containerProperties.setObservationEnabled(true);
			return new ConcurrentPulsarListenerContainerFactory<>(pulsarConsumerFactory, containerProperties);
		}

		@Bean
		PulsarListenerObservationConvention pulsarListenerConvention() {
			return new DefaultPulsarListenerObservationConvention() {
				@Override
				public KeyValues getLowCardinalityKeyValues(PulsarMessageReceiverContext context) {
					// Only add the extra tag for the 1st listener
					if (context.getListenerId().equals(OBS2_ID)) {
						return super.getLowCardinalityKeyValues(context);
					}
					return super.getLowCardinalityKeyValues(context).and(RECEIVER_EXTRA_TAG, context.getListenerId());
				}
			};
		}

		@Bean
		PulsarAdministration pulsarAdministration() {
			return new PulsarAdministration(PulsarTestContainerSupport.getHttpServiceUrl());
		}

		@Bean
		SimpleTracer simpleTracer() {
			return new SimpleTracer();
		}

		@Bean
		MeterRegistry meterRegistry() {
			return new SimpleMeterRegistry();
		}

		@Bean
		ObservationRegistry observationRegistry(Tracer tracer, Propagator propagator, MeterRegistry meterRegistry) {
			TestObservationRegistry observationRegistry = TestObservationRegistry.create();
			observationRegistry.observationConfig().observationHandler(
					// Composite will pick the first matching handler
					new ObservationHandler.FirstMatchingCompositeObservationHandler(
							// This is responsible for creating a child span on the sender
							// side
							new PropagatingSenderTracingObservationHandler<>(tracer, propagator),
							// This is responsible for creating a span on the receiver
							// side
							new PropagatingReceiverTracingObservationHandler<>(tracer, propagator),
							// This is responsible for creating a default span
							new DefaultTracingObservationHandler(tracer)))
					.observationHandler(new DefaultMeterObservationHandler(meterRegistry));
			return observationRegistry;
		}

		@Bean
		Propagator propagator(Tracer tracer) {
			return new Propagator() {

				// Headers required for tracing propagation
				@Override
				public List<String> fields() {
					return Arrays.asList(TAG1, TAG2);
				}

				// Called on the producer side when the message is being sent
				@Override
				public <C> void inject(TraceContext context, @Nullable C carrier, Setter<C> setter) {
					setter.set(carrier, TAG1, TAG1_VALUE);
					setter.set(carrier, TAG2, TAG2_VALUE);
				}

				// Called on the consumer side when the message is consumed
				@Override
				public <C> Span.Builder extract(C carrier, Getter<C> getter) {
					String tag1Value = getter.get(carrier, TAG1);
					String tag2Value = getter.get(carrier, TAG2);
					return tracer.spanBuilder().tag(TAG1, tag1Value).tag(TAG2, tag2Value);
				}
			};
		}

		@Bean
		ObservationTestAppListeners observationTestAppListeners(PulsarTemplate<String> pulsarTemplate) {
			return new ObservationTestAppListeners(pulsarTemplate);
		}

	}

	static class ObservationTestAppListeners {

		private PulsarTemplate<String> template;

		private Message<String> message;

		CountDownLatch latch = new CountDownLatch(1);

		ObservationTestAppListeners(PulsarTemplate<String> template) {
			this.template = template;
		}

		@PulsarListener(id = "obs1-id", properties = { "subscriptionName=obsTest-sub1", "topicNames=obs1-topic" })
		void listen1(Message<String> message) throws PulsarClientException {
			this.template.send("obs2-topic", message.getValue());
		}

		@PulsarListener(id = "obs2-id", properties = { "subscriptionName=obsTest-sub2", "topicNames=obs2-topic" })
		void listen2(Message<String> message) {
			this.message = message;
			this.latch.countDown();
		}

	}

}
