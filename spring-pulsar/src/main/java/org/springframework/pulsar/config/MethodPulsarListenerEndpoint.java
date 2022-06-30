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

package org.springframework.pulsar.config;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.commons.logging.LogFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.core.log.LogAccessor;
import org.springframework.expression.BeanResolver;
import org.springframework.lang.Nullable;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.pulsar.listener.DefaultPulsarMessageListenerContainer;
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
 * @author Soby Chacko
 */
public class MethodPulsarListenerEndpoint<V> extends AbstractPulsarListenerEndpoint<V> {


	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

	private Object bean;

	private Method method;
	private MessageHandlerMethodFactory messageHandlerMethodFactory;

	private SmartMessageConverter messagingConverter;

	public void setBean(Object bean) {
		this.bean = bean;
	}

	public Object getBean() {
		return this.bean;
	}

	/**
	 * Set the method to invoke to process a message managed by this endpoint.
	 *
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
		final HandlerAdapter handlerMethod = configureListenerAdapter(messageListener);
		messageListener.setHandlerMethod(handlerMethod);

		//Since we have access to the handler method here, check if we can type infer the Schema used.

		//TODO: filter out the payload type by excluding Consumer, Message, Messages etc.

		final MethodParameter[] methodParameters = handlerMethod.getInvokerHandlerMethod().getMethodParameters();
		MethodParameter methodParameter = null;
		final Optional<MethodParameter> parameter = Arrays.stream(methodParameters).filter(
				methodParameter1 -> !methodParameter1.getParameterType().equals(Consumer.class)).findFirst();
		final long count = Arrays.stream(methodParameters).filter(methodParameter1 -> !methodParameter1.getParameterType().equals(Consumer.class)).count();
		Assert.isTrue(count == 1, "More than 1 expected payload types found");
		if (parameter.isPresent()) {
			methodParameter = parameter.get();
		}


		final DefaultPulsarMessageListenerContainer<?> containerInstance = (DefaultPulsarMessageListenerContainer<?>) container;
		final PulsarContainerProperties pulsarContainerProperties = containerInstance.getPulsarContainerProperties();
		final SchemaType schemaType = pulsarContainerProperties.getSchemaType();
		if (schemaType != SchemaType.NONE) {

			switch (schemaType) {
				case STRING -> pulsarContainerProperties.setSchema(Schema.STRING);
				case BYTES -> pulsarContainerProperties.setSchema(Schema.BYTES);
				case INT8 -> pulsarContainerProperties.setSchema(Schema.INT8);
				case INT16 -> pulsarContainerProperties.setSchema(Schema.INT16);
				case INT32 -> pulsarContainerProperties.setSchema(Schema.INT32);
				case INT64 -> pulsarContainerProperties.setSchema(Schema.INT64);
				case BOOLEAN -> pulsarContainerProperties.setSchema(Schema.BOOL);
				case DATE -> pulsarContainerProperties.setSchema(Schema.DATE);
				case DOUBLE -> pulsarContainerProperties.setSchema(Schema.DOUBLE);
				case FLOAT -> pulsarContainerProperties.setSchema(Schema.FLOAT);
				case INSTANT -> pulsarContainerProperties.setSchema(Schema.INSTANT);
				case LOCAL_DATE -> pulsarContainerProperties.setSchema(Schema.LOCAL_DATE);
				case LOCAL_DATE_TIME -> pulsarContainerProperties.setSchema(Schema.LOCAL_DATE_TIME);
				case LOCAL_TIME -> pulsarContainerProperties.setSchema(Schema.LOCAL_TIME);
				case JSON -> {
					final Schema<?> requiredSchema = getRequiredSchema(methodParameters[0], pulsarContainerProperties);
					pulsarContainerProperties.setSchema(requiredSchema);
				}
			}
		}
		else {
			if (methodParameter != null) {
				final Class<?> type = methodParameter.getParameter().getType();

				String typeName = null;
				if (type.isAssignableFrom(List.class) || type.isAssignableFrom(Message.class) || type.isAssignableFrom(Messages.class)) {
					final ResolvableType resolvableType = ResolvableType.forMethodParameter(methodParameters[0]);
					final ResolvableType generic = resolvableType.getGeneric(0);
					if (generic.getRawClass() != null) {
						typeName = generic.getRawClass().getName();
					}
				}
				if (typeName == null) {
					typeName = type.getName();
				}
				switch (typeName) {
					case "java.lang.String" -> pulsarContainerProperties.setSchema(Schema.STRING);
					case "[B" -> pulsarContainerProperties.setSchema(Schema.BYTES);
					case "java.lang.Byte", "byte" -> pulsarContainerProperties.setSchema(Schema.INT8);
					case "java.lang.Short", "short" -> pulsarContainerProperties.setSchema(Schema.INT16);
					case "java.lang.Integer", "int" -> pulsarContainerProperties.setSchema(Schema.INT32);
					case "java.lang.Long", "long" -> pulsarContainerProperties.setSchema(Schema.INT64);
					case "java.lang.Boolean", "boolean" -> pulsarContainerProperties.setSchema(Schema.BOOL);
					case "java.nio.ByteBuffer" -> pulsarContainerProperties.setSchema(Schema.BYTEBUFFER);
					case "java.util.Date" -> pulsarContainerProperties.setSchema(Schema.DATE);
					case "java.lang.Double", "double" -> pulsarContainerProperties.setSchema(Schema.DOUBLE);
					case "java.lang.Float", "float" -> pulsarContainerProperties.setSchema(Schema.FLOAT);
					case "java.time.Instant" -> pulsarContainerProperties.setSchema(Schema.INSTANT);
					case "java.time.LocalDate" -> pulsarContainerProperties.setSchema(Schema.LOCAL_DATE);
					case "java.time.LocalDateTime" -> pulsarContainerProperties.setSchema(Schema.LOCAL_DATE_TIME);
					case "java.time.LocalTime" -> pulsarContainerProperties.setSchema(Schema.LOCAL_TIME);
				}
			}
		}
		final SchemaType type = pulsarContainerProperties.getSchema().getSchemaInfo().getType();
		pulsarContainerProperties.setSchemaType(type);

		return messageListener;
	}

	private Schema<?> getRequiredSchema(MethodParameter methodParameter, PulsarContainerProperties pulsarContainerProperties) {
		final ResolvableType resolvableType = ResolvableType.forMethodParameter(methodParameter);
		final Class<?> rawClass = resolvableType.getRawClass();
		return JSONSchema.of(rawClass);
	}

	protected HandlerAdapter configureListenerAdapter(PulsarMessagingMessageListenerAdapter<V> messageListener) {
		InvocableHandlerMethod invocableHandlerMethod =
				this.messageHandlerMethodFactory.createInvocableHandlerMethod(getBean(), getMethod());
		return new HandlerAdapter(invocableHandlerMethod);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
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


}
