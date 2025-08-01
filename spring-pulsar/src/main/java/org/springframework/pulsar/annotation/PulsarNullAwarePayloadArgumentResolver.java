/*
 * Copyright 2023-present the original author or authors.
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

package org.springframework.pulsar.annotation;

import java.util.List;

import org.jspecify.annotations.Nullable;

import org.springframework.core.MethodParameter;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.annotation.support.PayloadMethodArgumentResolver;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.pulsar.support.PulsarNull;

/**
 * A {@link PayloadMethodArgumentResolver} that can properly decode {@link PulsarNull}
 * payloads into null.
 *
 * @author Chris Bono
 * @since 1.0.1
 */
public class PulsarNullAwarePayloadArgumentResolver extends PayloadMethodArgumentResolver {

	PulsarNullAwarePayloadArgumentResolver(MessageConverter messageConverter) {
		super(messageConverter);
	}

	@Override
	public @Nullable Object resolveArgument(MethodParameter parameter, Message<?> message) throws Exception {
		if (message == null) {
			message = new GenericMessage<>(PulsarNull.INSTANCE);
		}
		Object resolved = super.resolveArgument(parameter, message);
		// Replace 'PulsarNull' elements w/ 'null'
		if (resolved instanceof List<?> list) {
			for (int i = 0; i < list.size(); i++) {
				if (list.get(i) instanceof PulsarNull) {
					list.set(i, null);
				}
			}
		}
		return resolved;
	}

	@Override
	protected boolean isEmptyPayload(@Nullable Object payload) {
		return payload == null || payload instanceof PulsarNull;
	}

}
