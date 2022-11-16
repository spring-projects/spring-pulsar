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

package org.springframework.pulsar.listener.reactive;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumer;
import org.apache.pulsar.reactive.client.api.ReactiveMessagePipeline;
import org.apache.pulsar.reactive.client.api.ReactiveMessagePipelineBuilder;
import org.apache.pulsar.reactive.client.internal.api.ApiImplementationFactory;

import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.core.reactive.ReactiveMessageConsumerBuilderCustomizer;
import org.springframework.pulsar.core.reactive.ReactivePulsarConsumerFactory;
import org.springframework.util.CollectionUtils;

/**
 * Default implementation for {@link ReactivePulsarMessageListenerContainer}.
 *
 * @param <T> message type.
 * @author Christophe Bornet
 */
public non-sealed class DefaultReactivePulsarMessageListenerContainer<T>
		implements ReactivePulsarMessageListenerContainer<T> {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private final ReactivePulsarConsumerFactory<T> pulsarConsumerFactory;

	private final ReactivePulsarContainerProperties<T> pulsarContainerProperties;

	private boolean autoStartup = true;

	private final Object lifecycleMonitor = new Object();

	private final AtomicBoolean running = new AtomicBoolean(false);

	private ReactiveMessageConsumerBuilderCustomizer<T> consumerCustomizer;

	private ReactiveMessagePipeline pipeline;

	public DefaultReactivePulsarMessageListenerContainer(ReactivePulsarConsumerFactory<T> pulsarConsumerFactory,
			ReactivePulsarContainerProperties<T> pulsarContainerProperties) {
		this.pulsarConsumerFactory = pulsarConsumerFactory;
		this.pulsarContainerProperties = pulsarContainerProperties;
	}

	public ReactivePulsarConsumerFactory<T> getReactivePulsarConsumerFactory() {
		return this.pulsarConsumerFactory;
	}

	public ReactivePulsarContainerProperties<T> getContainerProperties() {
		return this.pulsarContainerProperties;
	}

	@Override
	public boolean isRunning() {
		return this.running.get();
	}

	protected void setRunning(boolean running) {
		this.running.set(running);
	}

	@Override
	public void setupMessageHandler(ReactivePulsarMessageHandler messageHandler) {
		this.pulsarContainerProperties.setMessageHandler(messageHandler);
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	@Override
	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	public ReactiveMessageConsumerBuilderCustomizer<T> getConsumerCustomizer() {
		return this.consumerCustomizer;
	}

	@Override
	public void setConsumerCustomizer(ReactiveMessageConsumerBuilderCustomizer<T> consumerCustomizer) {
		this.consumerCustomizer = consumerCustomizer;
	}

	@Override
	public final void start() {
		synchronized (this.lifecycleMonitor) {
			if (!isRunning()) {
				Objects.requireNonNull(this.pulsarContainerProperties.getMessageHandler(),
						"A ReactivePulsarMessageHandler must be provided");
				doStart();
			}
		}
	}

	@Override
	public void stop() {
		synchronized (this.lifecycleMonitor) {
			if (isRunning()) {
				doStop();
			}
		}
	}

	private void doStart() {
		setRunning(true);
		this.pipeline = startPipeline(this.pulsarContainerProperties);
	}

	public void doStop() {
		try {
			this.logger.info("Closing Pulsar Reactive pipeline.");
			this.pipeline.close();
		}
		catch (Exception e) {
			this.logger.error(e, () -> "Error closing Pulsar Reactive pipeline.");
		}
		finally {
			setRunning(false);
		}
	}

	@SuppressWarnings({ "unchecked" })
	private ReactiveMessagePipeline startPipeline(ReactivePulsarContainerProperties<T> containerProperties) {
		ReactiveMessageConsumerBuilderCustomizer<T> customizer = (builder) -> {
			if (containerProperties.getSubscriptionType() != null) {
				builder.subscriptionType(containerProperties.getSubscriptionType());
			}
			if (containerProperties.getSubscriptionName() != null) {
				builder.subscriptionName(containerProperties.getSubscriptionName());
			}
			if (!CollectionUtils.isEmpty(containerProperties.getTopics())) {
				builder.topicNames(new ArrayList<>(containerProperties.getTopics()));
			}
			if (containerProperties.getTopicsPattern() != null) {
				builder.topicsPattern(containerProperties.getTopicsPattern());
			}
		};

		List<ReactiveMessageConsumerBuilderCustomizer<T>> customizers = new ArrayList<>();
		customizers.add(customizer);
		if (this.consumerCustomizer != null) {
			customizers.add(this.consumerCustomizer);
		}

		ReactiveMessageConsumer<T> consumer = getReactivePulsarConsumerFactory()
				.createConsumer(containerProperties.getSchema(), customizers);
		ReactiveMessagePipelineBuilder<T> pipelineBuilder = ApiImplementationFactory
				.createReactiveMessageHandlerPipelineBuilder(consumer);
		Object messageHandler = containerProperties.getMessageHandler();
		ReactiveMessagePipeline pipeline;
		if (messageHandler instanceof ReactivePulsarStreamingHandler<?>) {
			pipeline = pipelineBuilder
					.streamingMessageHandler(((ReactivePulsarStreamingHandler<T>) messageHandler)::received).build();
		}
		else {
			ReactiveMessagePipelineBuilder.OneByOneMessagePipelineBuilder<T> messagePipelineBuilder = pipelineBuilder
					.messageHandler(((ReactivePulsarOneByOneMessageHandler<T>) messageHandler)::received)
					.handlingTimeout(containerProperties.getHandlingTimeout());
			if (containerProperties.getConcurrency() > 0) {
				pipeline = messagePipelineBuilder.concurrent().concurrency(containerProperties.getConcurrency())
						.maxInflight(containerProperties.getMaxInFlight()).build();
			}
			else {
				pipeline = pipelineBuilder.build();
			}
		}
		pipeline.start();
		return pipeline;
	}

}
