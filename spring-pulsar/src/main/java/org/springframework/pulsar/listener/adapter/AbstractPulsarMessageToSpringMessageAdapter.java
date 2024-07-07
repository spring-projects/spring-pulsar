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

package org.springframework.pulsar.listener.adapter;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.List;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.Reader;

import org.springframework.context.expression.MapAccessor;
import org.springframework.core.MethodParameter;
import org.springframework.core.log.LogAccessor;
import org.springframework.expression.BeanResolver;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.expression.spel.support.StandardTypeConverter;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.pulsar.support.converter.PulsarMessageConverter;
import org.springframework.pulsar.support.converter.PulsarRecordMessageConverter;
import org.springframework.pulsar.support.header.JsonPulsarHeaderMapper;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * An abstract {@link org.apache.pulsar.client.api.MessageListener} adapter providing the
 * necessary infrastructure to extract the payload from a Pulsar message.
 *
 * @param <V> payload type.
 * @author Soby Chacko
 * @author Christophe Bornet
 */
public abstract class AbstractPulsarMessageToSpringMessageAdapter<V> {

	private static final SpelExpressionParser PARSER = new SpelExpressionParser();

	private final Object bean;

	protected final LogAccessor logger = new LogAccessor(getClass());

	private final Type inferredType;

	private final StandardEvaluationContext evaluationContext = new StandardEvaluationContext();

	private HandlerAdapter handlerMethod;

	private boolean headerFound = false;

	private boolean simpleExtraction = false;

	private boolean isPulsarMessageList;

	private boolean isSpringMessageList;

	private boolean isSpringMessageFlux;

	private boolean isSpringMessage;

	private boolean isConsumerRecords;

	private boolean converterSet;

	private PulsarMessageConverter<V> messageConverter;

	private Type fallbackType = Object.class;

	public AbstractPulsarMessageToSpringMessageAdapter(Object bean, Method method, ObjectMapper objectMapper) {
		this.bean = bean;
		this.messageConverter = new PulsarRecordMessageConverter<V>(
				JsonPulsarHeaderMapper.builder().objectMapper(objectMapper).build());
		this.inferredType = determineInferredType(method);
	}

	public void setMessageConverter(PulsarMessageConverter<V> messageConverter) {
		this.messageConverter = messageConverter;
		this.converterSet = true;
	}

	protected final PulsarMessageConverter<V> getMessageConverter() {
		return this.messageConverter;
	}

	public void setMessagingConverter(SmartMessageConverter messageConverter) {
		Assert.isTrue(!this.converterSet, "Cannot set the SmartMessageConverter when setting the messageConverter, "
				+ "add the SmartConverter to the message converter instead");
		((PulsarRecordMessageConverter<V>) this.messageConverter).setMessagingConverter(messageConverter);
	}

	protected Type getType() {
		return this.inferredType == null ? this.fallbackType : this.inferredType;
	}

	public void setFallbackType(Class<?> fallbackType) {
		this.fallbackType = fallbackType;
	}

	public void setHandlerMethod(HandlerAdapter handlerMethod) {
		this.handlerMethod = handlerMethod;
	}

	protected boolean isPulsarMessageList() {
		return this.isPulsarMessageList;
	}

	public void setBeanResolver(BeanResolver beanResolver) {
		this.evaluationContext.setBeanResolver(beanResolver);
		this.evaluationContext.setTypeConverter(new StandardTypeConverter());
		this.evaluationContext.addPropertyAccessor(new MapAccessor());
	}

	protected boolean isMessageList() {
		return this.isSpringMessageList;
	}

	protected boolean isSpringMessageFlux() {
		return this.isSpringMessageFlux;
	}

	protected org.springframework.messaging.Message<?> toMessagingMessage(Message<V> record, Consumer<V> consumer) {
		return getMessageConverter().toMessage(record, consumer, getType());
	}

	protected org.springframework.messaging.Message<?> toMessagingMessageFromReader(Message<V> record,
			Reader<V> reader) {
		return getMessageConverter().toMessageFromReader(record, reader, getType());
	}

	protected final Object invokeHandler(org.springframework.messaging.Message<?> message, Object... providedArgs) {
		try {
			return this.handlerMethod.invoke(message, providedArgs);
		}
		catch (Exception ex) {
			throw new MessageConversionException("Cannot handle message", ex);
		}
	}

	protected Type determineInferredType(Method method) { // NOSONAR complexity
		if (method == null) {
			return null;
		}
		Type genericParameterType = null;

		boolean pulsarMessageFound = false;
		boolean collectionFound = false;

		for (int i = 0; i < method.getParameterCount(); i++) {
			MethodParameter methodParameter = new MethodParameter(method, i);
			Type parameterType = methodParameter.getGenericParameterType();

			if (methodParameter.hasParameterAnnotation(Header.class)) {
				this.headerFound = true;
			}
			else if (parameterIsType(parameterType, org.springframework.messaging.Message.class)) {
				this.isSpringMessage = true;
			}
			else if (parameterIsType(parameterType, Message.class)) {
				pulsarMessageFound = true;
			}
			else if (isMultipleMessageType(parameterType)) {
				collectionFound = true;
			}
		}

		if (!this.headerFound && !this.isSpringMessage && !pulsarMessageFound && !collectionFound) {
			this.simpleExtraction = true;
		}

		for (int i = 0; i < method.getParameterCount(); i++) {
			MethodParameter methodParameter = new MethodParameter(method, i);
			/*
			 * We're looking for a single non-annotated parameter, or one annotated
			 * with @Payload. We ignore parameters with type Message, Consumer, Ack,
			 * ConsumerRecord because they are not involved with conversion.
			 */
			Type parameterType = methodParameter.getGenericParameterType();
			boolean isNotConvertible = parameterIsType(parameterType, Message.class);

			if (!isNotConvertible && !isMessageWithNoTypeInfo(parameterType)
					&& (methodParameter.getParameterAnnotations().length == 0
							|| methodParameter.hasParameterAnnotation(Payload.class))) {
				if (genericParameterType == null) {
					genericParameterType = extractGenericParameterTypFromMethodParameter(methodParameter);
				}
				else {
					this.logger.debug(() -> "Ambiguous parameters for target payload for method " + method
							+ "; no inferred type available");
					break;
				}
			}
		}
		return genericParameterType;
	}

	/**
	 * Determines if a type is one that holds multiple messages.
	 * @param type the type to check
	 * @return true if the type is a {@link List} or a {@link Messages}, false otherwise
	 */
	protected boolean isMultipleMessageType(Type type) {
		return parameterIsType(type, List.class) || parameterIsType(type, Messages.class);
	}

	private Type extractGenericParameterTypFromMethodParameter(MethodParameter methodParameter) {
		Type genericParameterType = methodParameter.getGenericParameterType();
		if (genericParameterType instanceof ParameterizedType parameterizedType) {
			if (parameterizedType.getRawType().equals(org.springframework.messaging.Message.class)) {
				genericParameterType = ((ParameterizedType) genericParameterType).getActualTypeArguments()[0];
			}
			else if (parameterizedType.getRawType().equals(List.class)
					&& parameterizedType.getActualTypeArguments().length == 1) {

				Type paramType = parameterizedType.getActualTypeArguments()[0];
				this.isPulsarMessageList = paramType instanceof ParameterizedType
						&& ((ParameterizedType) paramType).getRawType().equals(Message.class);
				boolean messageHasGeneric = paramType instanceof ParameterizedType
						&& ((ParameterizedType) paramType).getRawType()
							.equals(org.springframework.messaging.Message.class);
				this.isSpringMessageList = paramType.equals(org.springframework.messaging.Message.class)
						|| messageHasGeneric;
				if (messageHasGeneric) {
					genericParameterType = ((ParameterizedType) paramType).getActualTypeArguments()[0];
				}

				if (!this.isSpringMessageList && !this.isPulsarMessageList && !isHeaderFound()) {
					this.simpleExtraction = true;
				}
			}
			else if (isFlux(parameterizedType.getRawType()) && parameterizedType.getActualTypeArguments().length == 1) {

				Type paramType = parameterizedType.getActualTypeArguments()[0];
				boolean messageHasGeneric = paramType instanceof ParameterizedType
						&& ((ParameterizedType) paramType).getRawType()
							.equals(org.springframework.messaging.Message.class);
				this.isSpringMessageFlux = paramType.equals(org.springframework.messaging.Message.class)
						|| messageHasGeneric;
				if (messageHasGeneric) {
					genericParameterType = ((ParameterizedType) paramType).getActualTypeArguments()[0];
				}
			}
			else {
				this.isConsumerRecords = parameterizedType.getRawType().equals(Messages.class);
			}
		}
		return genericParameterType;
	}

	/**
	 * Determine if the type is a reactive Flux.
	 * @param type type to check
	 * @return false as the imperative side does not know about Flux
	 */
	protected boolean isFlux(Type type) {
		return false;
	}

	protected boolean parameterIsType(Type parameterType, Type type) {
		if (parameterType instanceof ParameterizedType parameterizedType) {
			Type rawType = parameterizedType.getRawType();
			if (rawType.equals(type)) {
				return true;
			}
		}
		return parameterType.equals(type);
	}

	private boolean isMessageWithNoTypeInfo(Type parameterType) {
		if (parameterType instanceof ParameterizedType parameterizedType) {
			Type rawType = parameterizedType.getRawType();
			if (rawType.equals(org.springframework.messaging.Message.class)) {
				return parameterizedType.getActualTypeArguments()[0] instanceof WildcardType;
			}
		}
		return parameterType.equals(org.springframework.messaging.Message.class);
	}

	public boolean isSimpleExtraction() {
		return this.simpleExtraction;
	}

	public boolean isConsumerRecords() {
		return this.isConsumerRecords;
	}

	public boolean isHeaderFound() {
		return this.headerFound;
	}

	public boolean isSpringMessage() {
		return this.isSpringMessage;
	}

}
