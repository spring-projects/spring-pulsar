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

package org.springframework.pulsar.listener.adapter;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;

import org.springframework.context.expression.MapAccessor;
import org.springframework.core.MethodParameter;
import org.springframework.core.log.LogAccessor;
import org.springframework.expression.BeanResolver;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.expression.spel.support.StandardTypeConverter;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.pulsar.listener.Acknowledgement;
import org.springframework.pulsar.support.converter.PulsarMessagingMessageConverter;
import org.springframework.pulsar.support.converter.PulsarRecordMessageConverter;
import org.springframework.util.Assert;

/**
 * An abstract {@link org.apache.pulsar.client.api.MessageListener} adapter providing the
 * necessary infrastructure to extract the payload from a Pulsar message.
 *
 * @param <V> payload type.
 * @author Soby Chacko
 */
public abstract class PulsarMessagingMessageListenerAdapter<V> {

	private static final SpelExpressionParser PARSER = new SpelExpressionParser();

	private final Object bean;

	protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

	private final Type inferredType;

	private final StandardEvaluationContext evaluationContext = new StandardEvaluationContext();

	private HandlerAdapter handlerMethod;

	private boolean conversionNeeded = true;

	private boolean messageReturnType;

	private boolean isConsumerRecordList;

	private boolean isMessageList;

	private boolean isConsumerRecords;

	private boolean converterSet;

	private PulsarRecordMessageConverter<V> messageConverter = new PulsarMessagingMessageConverter<V>();

	private Type fallbackType = Object.class;

	public PulsarMessagingMessageListenerAdapter(Object bean, Method method) {
		this.bean = bean;
		this.inferredType = determineInferredType(method);
	}

	public void setMessageConverter(PulsarRecordMessageConverter<V> messageConverter) {
		this.messageConverter = messageConverter;
		this.converterSet = true;
	}

	protected final PulsarRecordMessageConverter<V> getMessageConverter() {
		return this.messageConverter;
	}

	public void setMessagingConverter(SmartMessageConverter messageConverter) {
		Assert.isTrue(!this.converterSet, "Cannot set the SmartMessageConverter when setting the messageConverter, "
				+ "add the SmartConverter to the message converter instead");
		((PulsarMessagingMessageConverter<V>) this.messageConverter).setMessagingConverter(messageConverter);
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

	protected boolean isConsumerRecordList() {
		return this.isConsumerRecordList;
	}

	public boolean isConsumerRecords() {
		return this.isConsumerRecords;
	}

	public boolean isConversionNeeded() {
		return this.conversionNeeded;
	}

	public void setBeanResolver(BeanResolver beanResolver) {
		this.evaluationContext.setBeanResolver(beanResolver);
		this.evaluationContext.setTypeConverter(new StandardTypeConverter());
		this.evaluationContext.addPropertyAccessor(new MapAccessor());
	}

	protected boolean isMessageList() {
		return this.isMessageList;
	}

	protected org.springframework.messaging.Message<?> toMessagingMessage(Message<V> record, Consumer<V> consumer) {
		return getMessageConverter().toMessage(record, consumer, getType());
	}

	protected final Object invokeHandler(Object data, org.springframework.messaging.Message<?> message,
			Consumer<V> consumer, Acknowledgement acknowledgement) {

		try {
			return this.handlerMethod.invoke(message, data, consumer, acknowledgement);
			// if (data instanceof List && !this.isConsumerRecordList) {
			// return this.handlerMethod.invoke(message, consumer);
			// }
			// else {
			// return this.handlerMethod.invoke(message, data, consumer);
			// }
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
		int allowedBatchParameters = 1;
		int notConvertibleParameters = 0;

		for (int i = 0; i < method.getParameterCount(); i++) {
			MethodParameter methodParameter = new MethodParameter(method, i);
			/*
			 * We're looking for a single non-annotated parameter, or one annotated
			 * with @Payload. We ignore parameters with type Message, Consumer, Ack,
			 * ConsumerRecord because they are not involved with conversion.
			 */
			Type parameterType = methodParameter.getGenericParameterType();
			boolean isNotConvertible = parameterIsType(parameterType, Message.class);
			boolean isConsumer = parameterIsType(parameterType, Consumer.class);
			if (isNotConvertible) {
				notConvertibleParameters++;
			}
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
			else {
				if (isConsumer) {
					allowedBatchParameters++;
				}
				else {
					if (parameterType instanceof ParameterizedType
							&& ((ParameterizedType) parameterType).getRawType().equals(Consumer.class)) {
						allowedBatchParameters++;
					}
				}
			}
		}

		if (notConvertibleParameters == method.getParameterCount() && method.getReturnType().equals(void.class)) {
			this.conversionNeeded = false;
		}
		boolean validParametersForBatch = method.getGenericParameterTypes().length <= allowedBatchParameters;

		if (!validParametersForBatch) {
			String stateMessage = "A parameter of type '%s' must be the only parameter "
					+ "(except for an optional 'Acknowledgment' and/or 'Consumer' "
					+ "and/or '@Header(KafkaHeaders.GROUP_ID) String groupId'";
		}
		this.messageReturnType = returnTypeMessageOrCollectionOf(method);
		return genericParameterType;
	}

	private Type extractGenericParameterTypFromMethodParameter(MethodParameter methodParameter) {
		Type genericParameterType = methodParameter.getGenericParameterType();
		if (genericParameterType instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) genericParameterType;
			if (parameterizedType.getRawType().equals(org.springframework.messaging.Message.class)) {
				genericParameterType = ((ParameterizedType) genericParameterType).getActualTypeArguments()[0];
			}
			else if (parameterizedType.getRawType().equals(List.class)
					&& parameterizedType.getActualTypeArguments().length == 1) {

				Type paramType = parameterizedType.getActualTypeArguments()[0];
				this.isConsumerRecordList = paramType.equals(Messages.class);
				boolean messageHasGeneric = paramType instanceof ParameterizedType && ((ParameterizedType) paramType)
						.getRawType().equals(org.springframework.messaging.Message.class);
				this.isMessageList = paramType.equals(org.springframework.messaging.Message.class) || messageHasGeneric;
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

	public static boolean returnTypeMessageOrCollectionOf(Method method) {
		Type returnType = method.getGenericReturnType();
		if (returnType.equals(org.springframework.messaging.Message.class)) {
			return true;
		}
		if (returnType instanceof ParameterizedType) {
			ParameterizedType prt = (ParameterizedType) returnType;
			Type rawType = prt.getRawType();
			if (rawType.equals(org.springframework.messaging.Message.class)) {
				return true;
			}
			if (rawType.equals(Collection.class)) {
				Type collectionType = prt.getActualTypeArguments()[0];
				if (collectionType.equals(org.springframework.messaging.Message.class)) {
					return true;
				}
				return collectionType instanceof ParameterizedType && ((ParameterizedType) collectionType).getRawType()
						.equals(org.springframework.messaging.Message.class);
			}
		}
		return false;

	}

	private boolean parameterIsType(Type parameterType, Type type) {
		if (parameterType instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) parameterType;
			Type rawType = parameterizedType.getRawType();
			if (rawType.equals(type)) {
				return true;
			}
		}
		return parameterType.equals(type);
	}

	private boolean isMessageWithNoTypeInfo(Type parameterType) {
		if (parameterType instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) parameterType;
			Type rawType = parameterizedType.getRawType();
			if (rawType.equals(org.springframework.messaging.Message.class)) {
				return parameterizedType.getActualTypeArguments()[0] instanceof WildcardType;
			}
		}
		return parameterType.equals(org.springframework.messaging.Message.class); // could
																					// be
																					// Message
																					// without
																					// a
																					// generic
																					// type
	}

}
