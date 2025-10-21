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

package org.springframework.pulsar.listener.adapter;

import org.jspecify.annotations.Nullable;

import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;

/**
 * A wrapper for either an {@link InvocableHandlerMethod} or
 * {@link DelegatingInvocableHandler}. All methods delegate to the underlying handler.
 *
 * @author Soby Chacko
 */
public class HandlerAdapter {

	private final @Nullable InvocableHandlerMethod invokerHandlerMethod;

	private final @Nullable DelegatingInvocableHandler delegatingHandler;

	/**
	 * Construct an instance with the provided method.
	 * @param invokerHandlerMethod the method.
	 */
	public HandlerAdapter(InvocableHandlerMethod invokerHandlerMethod) {
		this.invokerHandlerMethod = invokerHandlerMethod;
		this.delegatingHandler = null;
	}

	/**
	 * Construct an instance with the provided delegating handler.
	 * @param delegatingHandler the handler.
	 */
	public HandlerAdapter(DelegatingInvocableHandler delegatingHandler) {
		this.invokerHandlerMethod = null;
		this.delegatingHandler = delegatingHandler;
	}

	@SuppressWarnings("NullAway")
	public @Nullable Object invoke(Message<?> message, @Nullable Object... providedArgs) throws Exception {
		if (this.invokerHandlerMethod != null) {
			return this.invokerHandlerMethod.invoke(message, providedArgs);
		}
		Assert.notNull(this.delegatingHandler,
				() -> "invokerHandlerMethod and delegatingHandler are null - one must be specified");
		var delegateHandler = this.delegatingHandler;
		if (delegateHandler.hasDefaultHandler()) {
			// Needed to avoid returning raw Message which matches Object
			Object[] args = new Object[providedArgs.length + 1];
			args[0] = message.getPayload();
			System.arraycopy(providedArgs, 0, args, 1, providedArgs.length);
			return this.delegatingHandler.invoke(message, args);
		}
		return delegateHandler.invoke(message, providedArgs);
	}

	public String getMethodAsString(Object payload) {
		if (this.invokerHandlerMethod != null) {
			return this.invokerHandlerMethod.getMethod().toGenericString();
		}
		Assert.notNull(this.delegatingHandler,
				() -> "invokerHandlerMethod and delegatingHandler are null - one must be specified");
		return this.delegatingHandler.getMethodNameFor(payload);
	}

	public Object getBean() {
		if (this.invokerHandlerMethod != null) {
			return this.invokerHandlerMethod.getBean();
		}
		Assert.notNull(this.delegatingHandler,
				() -> "invokerHandlerMethod and delegatingHandler are null - one must be specified");
		return this.delegatingHandler.getBean();
	}

	private DelegatingInvocableHandler getRequiredDelegatingHandler() {
		Assert.notNull(this.delegatingHandler, () -> "delegatingHandler must not be null");
		return this.delegatingHandler;
	}

	public @Nullable InvocableHandlerMethod getInvokerHandlerMethod() {
		return this.invokerHandlerMethod;
	}

	public InvocableHandlerMethod requireNonNullInvokerHandlerMethod() {
		Assert.notNull(this.invokerHandlerMethod, () -> "invokerHandlerMethod must not be null");
		return this.invokerHandlerMethod;
	}

}
