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

package org.springframework.pulsar.config;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.expression.BeanResolver;
import org.springframework.lang.Nullable;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.pulsar.core.ReaderBuilderCustomizer;
import org.springframework.pulsar.core.SchemaResolver;
import org.springframework.pulsar.listener.Acknowledgement;
import org.springframework.pulsar.listener.adapter.AbstractPulsarMessageToSpringMessageAdapter;
import org.springframework.pulsar.listener.adapter.HandlerAdapter;
import org.springframework.pulsar.listener.adapter.PulsarRecordMessageToSpringMessageReaderAdapter;
import org.springframework.pulsar.reader.DefaultPulsarMessageReaderContainer;
import org.springframework.pulsar.reader.PulsarMessageReaderContainer;
import org.springframework.pulsar.reader.PulsarReaderContainerProperties;
import org.springframework.pulsar.support.MessageConverter;
import org.springframework.pulsar.support.converter.PulsarMessageConverter;
import org.springframework.util.Assert;

/**
 * A {@link PulsarReaderEndpoint} providing the method to invoke to process an incoming
 * message for this endpoint.
 *
 * @param <V> Message payload type
 * @author Soby Chacko
 */
public class MethodPulsarReaderEndpoint<V> extends AbstractPulsarReaderEndpoint<V> {

	private Object bean;

	private Method method;

	private SmartMessageConverter messagingConverter;

	private MessageHandlerMethodFactory messageHandlerMethodFactory;

	private ReaderBuilderCustomizer<?> readerBuilderCustomizer;

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

	@Override
	protected AbstractPulsarMessageToSpringMessageAdapter<V> createReaderListener(
			PulsarMessageReaderContainer container, @Nullable MessageConverter messageConverter) {
		AbstractPulsarMessageToSpringMessageAdapter<V> readerListener = createMessageListenerInstance(messageConverter);
		HandlerAdapter handlerMethod = configureListenerAdapter(readerListener);
		readerListener.setHandlerMethod(handlerMethod);

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

		DefaultPulsarMessageReaderContainer<?> containerInstance = (DefaultPulsarMessageReaderContainer<?>) container;
		PulsarReaderContainerProperties pulsarContainerProperties = containerInstance.getContainerProperties();
		SchemaResolver schemaResolver = pulsarContainerProperties.getSchemaResolver();
		SchemaType schemaType = pulsarContainerProperties.getSchemaType();
		ResolvableType messageType = resolvableType(messageParameter);
		schemaResolver.resolveSchema(schemaType, messageType).ifResolved(pulsarContainerProperties::setSchema);

		// Make sure the schemaType is updated to match the current schema
		if (pulsarContainerProperties.getSchema() != null) {
			SchemaType type = pulsarContainerProperties.getSchema().getSchemaInfo().getType();
			pulsarContainerProperties.setSchemaType(type);
		}

		// TODO: If no topic info is set on endpoint attempt to resolve via message type
		// TopicResolver topicResolver = pulsarContainerProperties.getTopicResolver();
		// boolean hasTopicInfo =
		// !ObjectUtils.isEmpty(pulsarContainerProperties.getTopics())
		// || StringUtils.hasText(pulsarContainerProperties.getTopicsPattern());
		// if (!hasTopicInfo) {
		// topicResolver.resolveTopic(null, messageType.getRawClass(), () -> null)
		// .ifResolved((topic) -> pulsarContainerProperties.setTopics(new String[] { topic
		// }));
		// }

		container.setReaderCustomizer(this.readerBuilderCustomizer);

		return readerListener;
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

	protected HandlerAdapter configureListenerAdapter(AbstractPulsarMessageToSpringMessageAdapter<V> messageListener) {
		InvocableHandlerMethod invocableHandlerMethod = this.messageHandlerMethodFactory
			.createInvocableHandlerMethod(getBean(), getMethod());
		return new HandlerAdapter(invocableHandlerMethod);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected AbstractPulsarMessageToSpringMessageAdapter<V> createMessageListenerInstance(
			@Nullable MessageConverter messageConverter) {

		AbstractPulsarMessageToSpringMessageAdapter<V> listener;
		PulsarRecordMessageToSpringMessageReaderAdapter<V> messageListener = new PulsarRecordMessageToSpringMessageReaderAdapter<>(
				this.bean, this.method);
		if (messageConverter instanceof PulsarMessageConverter) {
			messageListener.setMessageConverter((PulsarMessageConverter) messageConverter);
		}
		listener = messageListener;

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

	public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory messageHandlerMethodFactory) {
		this.messageHandlerMethodFactory = messageHandlerMethodFactory;
	}

	public void setReaderBuilderCustomizer(ReaderBuilderCustomizer<?> readerBuilderCustomizer) {
		this.readerBuilderCustomizer = readerBuilderCustomizer;
	}

}
