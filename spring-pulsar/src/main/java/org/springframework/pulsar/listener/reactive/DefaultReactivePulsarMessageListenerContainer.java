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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumer;
import org.apache.pulsar.reactive.client.api.ReactiveMessagePipeline;
import org.apache.pulsar.reactive.client.api.ReactiveMessagePipelineBuilder;
import org.apache.pulsar.reactive.client.internal.api.ApiImplementationFactory;

import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.core.reactive.ReactiveMessageConsumerBuilderCustomizer;
import org.springframework.pulsar.core.reactive.ReactivePulsarConsumerFactory;
import org.springframework.util.Assert;

/**
 * Default implementation for {@link ReactivePulsarMessageListenerContainer}.
 *
 * @param <T> message type.
 * @author Christophe Bornet
 */
public class DefaultReactivePulsarMessageListenerContainer<T> implements ReactivePulsarMessageListenerContainer {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private final ReactivePulsarConsumerFactory<T> pulsarConsumerFactory;

	private final ReactivePulsarContainerProperties pulsarContainerProperties;

	private boolean autoStartup = true;

	private int phase;

	private final Object lifecycleMonitor = new Object();

	private volatile boolean running = false;

	private ReactiveMessageConsumerBuilderCustomizer<T> consumerCustomizer;

	private ReactiveMessagePipeline pipeline;

	@SuppressWarnings("unchecked")
	public DefaultReactivePulsarMessageListenerContainer(ReactivePulsarConsumerFactory<? super T> pulsarConsumerFactory,
			ReactivePulsarContainerProperties pulsarContainerProperties) {
		this.pulsarConsumerFactory = (ReactivePulsarConsumerFactory<T>) pulsarConsumerFactory;
		this.pulsarContainerProperties = pulsarContainerProperties;
	}

	public ReactivePulsarConsumerFactory<T> getReactivePulsarConsumerFactory() {
		return this.pulsarConsumerFactory;
	}

	public ReactivePulsarContainerProperties getContainerProperties() {
		return this.pulsarContainerProperties;
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	protected void setRunning(boolean running) {
		this.running = running;
	}

	@Override
	public void setupMessageHandler(Object messageHandler) {
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

	public void setPhase(int phase) {
		this.phase = phase;
	}

	@Override
	public int getPhase() {
		return this.phase;
	}

	@Override
	public final void start() {
		synchronized (this.lifecycleMonitor) {
			if (!isRunning()) {
				Assert.state(
						this.pulsarContainerProperties.getMessageHandler() instanceof ReactivePulsarStreamingHandler<?>
								|| this.pulsarContainerProperties
										.getMessageHandler() instanceof ReactivePulsarMessageHandler<?>,
						() -> "A ReactivePulsarMessageHandler or ReactivePulsarStreamingHandler implementation must be provided");
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

	public ReactiveMessageConsumerBuilderCustomizer<T> getConsumerCustomizer() {
		return this.consumerCustomizer;
	}

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void setConsumerCustomizer(ReactiveMessageConsumerBuilderCustomizer consumerCustomizer) {
		this.consumerCustomizer = consumerCustomizer;
	}

	private void doStart() {
		this.pipeline = startPipeline(this.pulsarContainerProperties);
		setRunning(true);
	}

	public void doStop() {
		try {
			this.logger.info("Closing this reactive pipeline.");
			this.pipeline.close();
		}
		catch (Exception e) {
			this.logger.error(e, () -> "Error closing Pulsar Reactive pipeline.");
		}
	}

	@Override
	public void destroy() {

	}

	@SuppressWarnings({ "unchecked" })
	private ReactiveMessagePipeline startPipeline(ReactivePulsarContainerProperties containerProperties) {
		ReactiveMessageConsumerBuilderCustomizer<T> customizer = (builder) -> {
			if (containerProperties.getSubscriptionType() != null) {
				builder.subscriptionType(containerProperties.getSubscriptionType());
			}
			if (containerProperties.getSubscriptionName() != null) {
				builder.subscriptionName(containerProperties.getSubscriptionName());
			}
			if (containerProperties.getTopics() != null) {
				List<String> topicNames = Arrays.asList(containerProperties.getTopics());
				if (!topicNames.isEmpty()) {
					builder.topicNames(topicNames);
				}
			}
			if (containerProperties.getTopicsPattern() != null) {
				builder.topicsPattern(Pattern.compile(containerProperties.getTopicsPattern()));
			}
		};

		List<ReactiveMessageConsumerBuilderCustomizer<T>> customizers = new ArrayList<>();
		customizers.add(customizer);
		if (this.consumerCustomizer != null) {
			customizers.add(this.consumerCustomizer);
		}

		ReactiveMessageConsumer<T> consumer = getReactivePulsarConsumerFactory()
				.createConsumer((Schema<T>) containerProperties.getSchema(), customizers);
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
					.messageHandler(((ReactivePulsarMessageHandler<T>) messageHandler)::received)
					.handlingTimeout(Duration.ofMillis(containerProperties.getHandlingTimeoutMillis()));
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
