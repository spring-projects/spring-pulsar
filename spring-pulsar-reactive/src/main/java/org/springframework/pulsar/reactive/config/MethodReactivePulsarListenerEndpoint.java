/*
 * Copyright 2022-present the original author or authors.
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

package org.springframework.pulsar.reactive.config;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.common.schema.SchemaType;
import org.jspecify.annotations.Nullable;

import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.core.log.LogAccessor;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.pulsar.core.SchemaResolver;
import org.springframework.pulsar.core.TopicResolver;
import org.springframework.pulsar.listener.Acknowledgement;
import org.springframework.pulsar.listener.adapter.AbstractPulsarMessageToSpringMessageAdapter;
import org.springframework.pulsar.listener.adapter.HandlerAdapter;
import org.springframework.pulsar.reactive.core.ReactiveMessageConsumerBuilderCustomizer;
import org.springframework.pulsar.reactive.listener.DefaultReactivePulsarMessageListenerContainer;
import org.springframework.pulsar.reactive.listener.ReactivePulsarContainerProperties;
import org.springframework.pulsar.reactive.listener.ReactivePulsarMessageListenerContainer;
import org.springframework.pulsar.reactive.listener.adapter.PulsarReactiveOneByOneMessagingMessageListenerAdapter;
import org.springframework.pulsar.reactive.listener.adapter.PulsarReactiveStreamingMessagingMessageListenerAdapter;
import org.springframework.pulsar.support.MessageConverter;
import org.springframework.pulsar.support.converter.PulsarMessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.publisher.Flux;

/**
 * A {@link ReactivePulsarListenerEndpoint} providing the method to invoke to process an
 * incoming message for this endpoint.
 *
 * @param <V> Message payload type
 * @author Christophe Bornet
 * @author Chris Bono
 * @author Jihoon Kim
 */
public class MethodReactivePulsarListenerEndpoint<V> extends AbstractReactivePulsarListenerEndpoint<V> {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private @Nullable Object bean;

	private @Nullable Method method;

	private @Nullable ObjectMapper objectMapper;

	private @Nullable MessageHandlerMethodFactory messageHandlerMethodFactory;

	private @Nullable SmartMessageConverter messagingConverter;

	private @Nullable ReactiveMessageConsumerBuilderCustomizer<V> consumerCustomizer;

	private @Nullable DeadLetterPolicy deadLetterPolicy;

	public void setBean(Object bean) {
		this.bean = bean;
	}

	public @Nullable Object getBean() {
		return this.bean;
	}

	protected Object requireNonNullBean() {
		Assert.notNull(this.bean, "Bean must not be null");
		return this.bean;
	}

	/**
	 * Set the method to invoke to process a message managed by this endpoint.
	 * @param method the target method for the {@link #bean}.
	 */
	public void setMethod(Method method) {
		this.method = method;
	}

	public @Nullable Method getMethod() {
		return this.method;
	}

	protected Method requireNonNullMethod() {
		Assert.notNull(this.method, "Method must not be null");
		return this.method;
	}

	public void setMessageHandlerMethodFactory(@Nullable MessageHandlerMethodFactory messageHandlerMethodFactory) {
		this.messageHandlerMethodFactory = messageHandlerMethodFactory;
	}

	protected MessageHandlerMethodFactory requireNonNullMessageHandlerMethodFactory() {
		Assert.notNull(this.messageHandlerMethodFactory, "The messageHandlerMethodFactory must not be null");
		return this.messageHandlerMethodFactory;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected AbstractPulsarMessageToSpringMessageAdapter<V> createMessageHandler(
			ReactivePulsarMessageListenerContainer<V> container, @Nullable MessageConverter messageConverter) {
		var messageHandlerMethodFactory = requireNonNullMessageHandlerMethodFactory();
		AbstractPulsarMessageToSpringMessageAdapter<V> messageListener = createMessageListenerInstance(
				messageConverter);
		HandlerAdapter handlerMethod = configureListenerAdapter(messageListener, messageHandlerMethodFactory);
		messageListener.setHandlerMethod(handlerMethod);

		// Determine the single payload param to use
		var methodParameters = handlerMethod.requireNonNullInvokerHandlerMethod().getMethodParameters();
		var allPayloadParams = Arrays.stream(methodParameters)
			.filter(param -> !param.getParameterType().equals(Consumer.class)
					&& !param.getParameterType().equals(Acknowledgement.class)
					&& !param.hasParameterAnnotation(Header.class))
			.toList();
		Assert.isTrue(allPayloadParams.size() == 1, "Expected 1 payload types but found " + allPayloadParams);
		var messageParameter = allPayloadParams.stream()
			.findFirst()
			.orElseThrow(() -> new IllegalArgumentException("Unable to determine message parameter"));

		DefaultReactivePulsarMessageListenerContainer<Object> containerInstance = (DefaultReactivePulsarMessageListenerContainer<Object>) container;
		ReactivePulsarContainerProperties<Object> pulsarContainerProperties = containerInstance
			.getContainerProperties();

		// Resolve the schema using the reader schema type
		SchemaResolver schemaResolver = pulsarContainerProperties.getSchemaResolver();
		SchemaType schemaType = pulsarContainerProperties.getSchemaType();
		ResolvableType messageType = resolvableType(messageParameter);
		schemaResolver.resolveSchema(schemaType, messageType)
			.ifResolvedOrElse(pulsarContainerProperties::setSchema,
					(ex) -> this.logger
						.warn(() -> "Failed to resolve schema for type %s - will default to BYTES (due to: %s)"
							.formatted(schemaType, ex.getMessage())));

		// Attempt to make sure the schemaType is updated to match the resolved schema.
		// This can occur when the resolver returns a schema that is not necessarily of
		// the same type as the input scheme type (e.g. SchemaType.NONE uses the message
		// type to determine the schema.
		if (pulsarContainerProperties.getSchema() != null) {
			var schemaInfo = pulsarContainerProperties.getSchema().getSchemaInfo();
			if (schemaInfo != null) {
				pulsarContainerProperties.setSchemaType(schemaInfo.getType());
			}
		}

		// If no topic info is set on endpoint attempt to resolve via message type
		TopicResolver topicResolver = pulsarContainerProperties.getTopicResolver();
		boolean hasTopicInfo = pulsarContainerProperties.getTopicsPattern() != null
				|| !ObjectUtils.isEmpty(pulsarContainerProperties.getTopics());
		if (!hasTopicInfo) {
			topicResolver.resolveTopic(null, messageType.getRawClass(), () -> null)
				.ifResolved((topic) -> pulsarContainerProperties.setTopics(Collections.singleton(topic)));
		}

		ReactiveMessageConsumerBuilderCustomizer<V> customizer1 = b -> b.deadLetterPolicy(this.deadLetterPolicy);
		container.setConsumerCustomizer(b -> {
			if (this.consumerCustomizer != null) {
				this.consumerCustomizer.customize(b);
			}
			customizer1.customize(b);
		});

		return messageListener;
	}

	private ResolvableType resolvableType(MethodParameter methodParameter) {
		ResolvableType resolvableType = ResolvableType.forMethodParameter(methodParameter);
		Class<?> rawClass = resolvableType.getRawClass();
		if (rawClass != null && isContainerType(rawClass)) {
			resolvableType = resolvableType.getGeneric(0);
		}
		if (resolvableType.getRawClass() != null && (Message.class.isAssignableFrom(resolvableType.getRawClass())
				|| org.springframework.messaging.Message.class.isAssignableFrom(resolvableType.getRawClass()))) {
			resolvableType = resolvableType.getGeneric(0);
		}
		return resolvableType;
	}

	private boolean isContainerType(Class<?> rawClass) {
		return rawClass.isAssignableFrom(Flux.class) || rawClass.isAssignableFrom(List.class)
				|| rawClass.isAssignableFrom(Message.class) || rawClass.isAssignableFrom(Messages.class)
				|| rawClass.isAssignableFrom(org.springframework.messaging.Message.class);
	}

	protected HandlerAdapter configureListenerAdapter(AbstractPulsarMessageToSpringMessageAdapter<V> messageListener,
			MessageHandlerMethodFactory messageHandlerMethodFactory) {
		InvocableHandlerMethod invocableHandlerMethod = messageHandlerMethodFactory
			.createInvocableHandlerMethod(requireNonNullBean(), requireNonNullMethod());
		return new HandlerAdapter(invocableHandlerMethod);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected AbstractPulsarMessageToSpringMessageAdapter<V> createMessageListenerInstance(
			@Nullable MessageConverter messageConverter) {
		AbstractPulsarMessageToSpringMessageAdapter<V> listener;
		if (isFluxListener()) {
			listener = new PulsarReactiveStreamingMessagingMessageListenerAdapter<>(requireNonNullBean(),
					requireNonNullMethod());
		}
		else {
			listener = new PulsarReactiveOneByOneMessagingMessageListenerAdapter<>(requireNonNullBean(),
					requireNonNullMethod());
		}
		if (messageConverter instanceof PulsarMessageConverter pulsarMessageConverter) {
			listener.setMessageConverter(pulsarMessageConverter);
		}
		if (this.messagingConverter != null) {
			listener.setMessagingConverter(this.messagingConverter);
		}
		if (this.objectMapper != null) {
			listener.setObjectMapper(this.objectMapper);
		}
		var resolver = getBeanResolver();
		if (resolver != null) {
			listener.setBeanResolver(resolver);
		}
		return listener;
	}

	public void setObjectMapper(@Nullable ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	public void setMessagingConverter(@Nullable SmartMessageConverter messagingConverter) {
		this.messagingConverter = messagingConverter;
	}

	public void setDeadLetterPolicy(@Nullable DeadLetterPolicy deadLetterPolicy) {
		this.deadLetterPolicy = deadLetterPolicy;
	}

	public @Nullable ReactiveMessageConsumerBuilderCustomizer<V> getConsumerCustomizer() {
		return this.consumerCustomizer;
	}

	public void setConsumerCustomizer(@Nullable ReactiveMessageConsumerBuilderCustomizer<V> consumerCustomizer) {
		this.consumerCustomizer = consumerCustomizer;
	}

}
