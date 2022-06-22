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

import org.apache.commons.logging.LogFactory;

import org.springframework.core.log.LogAccessor;
import org.springframework.expression.BeanResolver;
import org.springframework.lang.Nullable;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.pulsar.listener.adapter.HandlerAdapter;
import org.springframework.pulsar.support.MessageConverter;
import org.springframework.pulsar.support.converter.PulsarBatchMessageConverter;
import org.springframework.pulsar.listener.adapter.PulsarBatchMessagingMessageListenerAdapter;
import org.springframework.pulsar.listener.PulsarMessageListenerContainer;
import org.springframework.pulsar.listener.adapter.PulsarMessagingMessageListenerAdapter;
import org.springframework.pulsar.support.converter.PulsarRecordMessageConverter;
import org.springframework.pulsar.listener.adapter.PulsarRecordMessagingMessageListenerAdapter;
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
		messageListener.setHandlerMethod(configureListenerAdapter(messageListener));

		return messageListener;

	}

	protected HandlerAdapter configureListenerAdapter(PulsarMessagingMessageListenerAdapter<V> messageListener) {
		InvocableHandlerMethod invocableHandlerMethod =
				this.messageHandlerMethodFactory.createInvocableHandlerMethod(getBean(), getMethod());
		return new HandlerAdapter(invocableHandlerMethod);
	}

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
