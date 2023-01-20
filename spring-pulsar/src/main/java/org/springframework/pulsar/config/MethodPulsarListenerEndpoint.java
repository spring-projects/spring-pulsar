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

package org.springframework.pulsar.config;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.commons.logging.LogFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.RedeliveryBackoff;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.core.log.LogAccessor;
import org.springframework.expression.BeanResolver;
import org.springframework.lang.Nullable;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.pulsar.core.SchemaResolver;
import org.springframework.pulsar.listener.Acknowledgement;
import org.springframework.pulsar.listener.ConcurrentPulsarMessageListenerContainer;
import org.springframework.pulsar.listener.PulsarConsumerErrorHandler;
import org.springframework.pulsar.listener.PulsarContainerProperties;
import org.springframework.pulsar.listener.PulsarMessageListenerContainer;
import org.springframework.pulsar.listener.adapter.HandlerAdapter;
import org.springframework.pulsar.listener.adapter.PulsarBatchMessagingMessageListenerAdapter;
import org.springframework.pulsar.listener.adapter.PulsarMessagingMessageListenerAdapter;
import org.springframework.pulsar.listener.adapter.PulsarRecordMessagingMessageListenerAdapter;
import org.springframework.pulsar.support.MessageConverter;
import org.springframework.pulsar.support.converter.PulsarBatchMessageConverter;
import org.springframework.pulsar.support.converter.PulsarRecordMessageConverter;
import org.springframework.util.Assert;

/**
 * A {@link PulsarListenerEndpoint} providing the method to invoke to process an incoming
 * message for this endpoint.
 *
 * @param <V> Message payload type
 * @author Soby Chacko
 * @author Alexander Preuß
 * @author Chris Bono
 */
public class MethodPulsarListenerEndpoint<V> extends AbstractPulsarListenerEndpoint<V> {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

	private Object bean;

	private Method method;

	private MessageHandlerMethodFactory messageHandlerMethodFactory;

	private SmartMessageConverter messagingConverter;

	private RedeliveryBackoff negativeAckRedeliveryBackoff;

	private RedeliveryBackoff ackTimeoutRedeliveryBackoff;

	private DeadLetterPolicy deadLetterPolicy;

	@SuppressWarnings("rawtypes")
	private PulsarConsumerErrorHandler pulsarConsumerErrorHandler;

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
	protected PulsarMessagingMessageListenerAdapter<V> createMessageListener(PulsarMessageListenerContainer container,
			@Nullable MessageConverter messageConverter) {
		Assert.state(this.messageHandlerMethodFactory != null,
				"Could not create message listener - MessageHandlerMethodFactory not set");
		PulsarMessagingMessageListenerAdapter<V> messageListener = createMessageListenerInstance(messageConverter);
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

		ConcurrentPulsarMessageListenerContainer<?> containerInstance = (ConcurrentPulsarMessageListenerContainer<?>) container;
		PulsarContainerProperties pulsarContainerProperties = containerInstance.getContainerProperties();
		SchemaResolver schemaResolver = pulsarContainerProperties.getSchemaResolver();
		SchemaType schemaType = pulsarContainerProperties.getSchemaType();
		ResolvableType messageType = resolvableType(messageParameter);
		Schema<?> schema = schemaResolver.getSchema(schemaType, messageType);
		if (schema != null) {
			pulsarContainerProperties.setSchema(schema);
		}
		// Make sure the schemaType is updated to match the current schema
		if (pulsarContainerProperties.getSchema() != null) {
			SchemaType type = pulsarContainerProperties.getSchema().getSchemaInfo().getType();
			pulsarContainerProperties.setSchemaType(type);
		}
		container.setNegativeAckRedeliveryBackoff(this.negativeAckRedeliveryBackoff);
		container.setAckTimeoutRedeliveryBackoff(this.ackTimeoutRedeliveryBackoff);
		container.setDeadLetterPolicy(this.deadLetterPolicy);
		container.setPulsarConsumerErrorHandler(this.pulsarConsumerErrorHandler);

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
		return rawClass.isAssignableFrom(List.class) || rawClass.isAssignableFrom(Message.class)
				|| rawClass.isAssignableFrom(Messages.class)
				|| rawClass.isAssignableFrom(org.springframework.messaging.Message.class);
	}

	protected HandlerAdapter configureListenerAdapter(PulsarMessagingMessageListenerAdapter<V> messageListener) {
		InvocableHandlerMethod invocableHandlerMethod = this.messageHandlerMethodFactory
				.createInvocableHandlerMethod(getBean(), getMethod());
		return new HandlerAdapter(invocableHandlerMethod);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected PulsarMessagingMessageListenerAdapter<V> createMessageListenerInstance(
			@Nullable MessageConverter messageConverter) {

		PulsarMessagingMessageListenerAdapter<V> listener;
		if (isBatchListener()) {
			PulsarBatchMessagingMessageListenerAdapter<V> messageListener = new PulsarBatchMessagingMessageListenerAdapter<V>(
					this.bean, this.method);
			if (messageConverter instanceof PulsarBatchMessageConverter) {
				messageListener.setBatchMessageConverter((PulsarBatchMessageConverter) messageConverter);
			}
			listener = messageListener;
		}
		else {
			PulsarRecordMessagingMessageListenerAdapter<V> messageListener = new PulsarRecordMessagingMessageListenerAdapter<V>(
					this.bean, this.method);
			if (messageConverter instanceof PulsarRecordMessageConverter) {
				messageListener.setMessageConverter((PulsarRecordMessageConverter) messageConverter);
			}
			listener = messageListener;
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

	public void setNegativeAckRedeliveryBackoff(RedeliveryBackoff negativeAckRedeliveryBackoff) {
		this.negativeAckRedeliveryBackoff = negativeAckRedeliveryBackoff;
	}

	public void setDeadLetterPolicy(DeadLetterPolicy deadLetterPolicy) {
		this.deadLetterPolicy = deadLetterPolicy;
	}

	@SuppressWarnings("rawtypes")
	public void setPulsarConsumerErrorHandler(PulsarConsumerErrorHandler pulsarConsumerErrorHandler) {
		this.pulsarConsumerErrorHandler = pulsarConsumerErrorHandler;
	}

	public void setAckTimeoutRedeliveryBackoff(RedeliveryBackoff ackTimeoutRedeliveryBackoff) {
		this.ackTimeoutRedeliveryBackoff = ackTimeoutRedeliveryBackoff;
	}

}
