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

package org.springframework.pulsar.reactive.config;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.expression.BeanResolver;
import org.springframework.lang.Nullable;
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

import reactor.core.publisher.Flux;

/**
 * A {@link ReactivePulsarListenerEndpoint} providing the method to invoke to process an
 * incoming message for this endpoint.
 *
 * @param <V> Message payload type
 * @author Christophe Bornet
 * @author Chris Bono
 */
public class MethodReactivePulsarListenerEndpoint<V> extends AbstractReactivePulsarListenerEndpoint<V> {

	private Object bean;

	private Method method;

	private MessageHandlerMethodFactory messageHandlerMethodFactory;

	private SmartMessageConverter messagingConverter;

	private ReactiveMessageConsumerBuilderCustomizer<V> consumerCustomizer;

	private DeadLetterPolicy deadLetterPolicy;

	public void setBean(Object bean) {
		this.bean = bean;
	}

	public Object getBean() {
		return this.bean;
	}

	/**
	 * Set the method to invoke to process a message managed by this endpoint.
	 * @param method the target method for the {@link #bean}.
	 */
	public void setMethod(Method method) {
		this.method = method;
	}

	public Method getMethod() {
		return this.method;
	}

	public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory messageHandlerMethodFactory) {
		this.messageHandlerMethodFactory = messageHandlerMethodFactory;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected AbstractPulsarMessageToSpringMessageAdapter<V> createMessageHandler(
			ReactivePulsarMessageListenerContainer<V> container, @Nullable MessageConverter messageConverter) {
		Assert.state(this.messageHandlerMethodFactory != null,
				"Could not create message listener - MessageHandlerMethodFactory not set");
		AbstractPulsarMessageToSpringMessageAdapter<V> messageListener = createMessageListenerInstance(
				messageConverter);
		HandlerAdapter handlerMethod = configureListenerAdapter(messageListener);
		messageListener.setHandlerMethod(handlerMethod);

		// Since we have access to the handler method here, check if we can type infer the
		// Schema used.

		// TODO: filter out the payload type by excluding Consumer, Message, Messages etc.

		MethodParameter[] methodParameters = handlerMethod.getInvokerHandlerMethod().getMethodParameters();
		MethodParameter messageParameter = null;
		Optional<MethodParameter> parameter = Arrays.stream(methodParameters)
				.filter(methodParameter1 -> !methodParameter1.getParameterType().equals(Consumer.class)
						|| !methodParameter1.getParameterType().equals(Acknowledgement.class)
						|| !methodParameter1.hasParameterAnnotation(Header.class))
				.findFirst();
		long count = Arrays.stream(methodParameters)
				.filter(methodParameter1 -> !methodParameter1.getParameterType().equals(Consumer.class)
						&& !methodParameter1.getParameterType().equals(Acknowledgement.class)
						&& !methodParameter1.hasParameterAnnotation(Header.class))
				.count();
		Assert.isTrue(count == 1, "More than 1 expected payload types found");
		if (parameter.isPresent()) {
			messageParameter = parameter.get();
		}

		DefaultReactivePulsarMessageListenerContainer<?> containerInstance = (DefaultReactivePulsarMessageListenerContainer<?>) container;
		ReactivePulsarContainerProperties<?> pulsarContainerProperties = containerInstance.getContainerProperties();
		SchemaResolver schemaResolver = pulsarContainerProperties.getSchemaResolver();
		SchemaType schemaType = pulsarContainerProperties.getSchemaType();
		ResolvableType messageType = resolvableType(messageParameter);
		schemaResolver.resolveSchema(schemaType, messageType)
				.ifResolved(schema -> pulsarContainerProperties.setSchema((Schema) schema));

		// Make sure the schemaType is updated to match the current schema
		if (pulsarContainerProperties.getSchema() != null) {
			SchemaType type = pulsarContainerProperties.getSchema().getSchemaInfo().getType();
			pulsarContainerProperties.setSchemaType(type);
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
		if (Message.class.isAssignableFrom(resolvableType.getRawClass())
				|| org.springframework.messaging.Message.class.isAssignableFrom(resolvableType.getRawClass())) {
			resolvableType = resolvableType.getGeneric(0);
		}
		return resolvableType;
	}

	private boolean isContainerType(Class<?> rawClass) {
		return rawClass.isAssignableFrom(Flux.class) || rawClass.isAssignableFrom(List.class)
				|| rawClass.isAssignableFrom(Message.class) || rawClass.isAssignableFrom(Messages.class)
				|| rawClass.isAssignableFrom(org.springframework.messaging.Message.class);
	}

	protected HandlerAdapter configureListenerAdapter(AbstractPulsarMessageToSpringMessageAdapter<V> messageListener) {
		InvocableHandlerMethod invocableHandlerMethod = this.messageHandlerMethodFactory
				.createInvocableHandlerMethod(getBean(), getMethod());
		return new HandlerAdapter(invocableHandlerMethod);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected AbstractPulsarMessageToSpringMessageAdapter<V> createMessageListenerInstance(
			@Nullable MessageConverter messageConverter) {

		AbstractPulsarMessageToSpringMessageAdapter<V> listener;
		if (isFluxListener()) {
			listener = new PulsarReactiveStreamingMessagingMessageListenerAdapter<>(this.bean, this.method);
		}
		else {
			listener = new PulsarReactiveOneByOneMessagingMessageListenerAdapter<>(this.bean, this.method);
		}

		if (messageConverter instanceof PulsarMessageConverter) {
			listener.setMessageConverter((PulsarMessageConverter) messageConverter);
		}
		if (this.messagingConverter != null) {
			listener.setMessagingConverter(this.messagingConverter);
		}
		BeanResolver resolver = getBeanResolver();
		if (resolver != null) {
			listener.setBeanResolver(resolver);
		}
		return listener;
	}

	public void setMessagingConverter(SmartMessageConverter messagingConverter) {
		this.messagingConverter = messagingConverter;
	}

	public void setDeadLetterPolicy(DeadLetterPolicy deadLetterPolicy) {
		this.deadLetterPolicy = deadLetterPolicy;
	}

	public void setConsumerCustomizer(ReactiveMessageConsumerBuilderCustomizer<V> consumerCustomizer) {
		this.consumerCustomizer = consumerCustomizer;
	}

}
