/*
 * Copyright 2012-2024 the original author or authors.
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

package org.springframework.pulsar.core;

import java.util.function.Supplier;

import org.springframework.lang.Nullable;

/**
 * Specialization of {@link TopicResolver} that is aware of expression resolution through
 * use of an {@link ExpressionResolver}.
 *
 * @author Jonas Geiregat
 */
public class ExpressionAwareTopicResolver implements TopicResolver {

	private final TopicResolver topicResolver;

	private final ExpressionResolver expressionResolver;

	public ExpressionAwareTopicResolver(TopicResolver topicResolver, ExpressionResolver expressionResolver) {
		this.topicResolver = topicResolver;
		this.expressionResolver = expressionResolver;
	}

	@Override
	public Resolved<String> resolveTopic(@Nullable String userSpecifiedTopic, Supplier<String> defaultTopicSupplier) {
		var resolved = this.topicResolver.resolveTopic(userSpecifiedTopic, defaultTopicSupplier);
		return resolved.map(e -> this.expressionResolver.resolve(e).resolveToString().value().orElse(e));
	}

	@Override
	public <T> Resolved<String> resolveTopic(@Nullable String userSpecifiedTopic, @Nullable T message,
			Supplier<String> defaultTopicSupplier) {
		var resolved = this.topicResolver.resolveTopic(userSpecifiedTopic, message, defaultTopicSupplier);
		return resolved.map(e -> this.expressionResolver.resolve(e).resolveToString().value().orElse(e));
	}

	@Override
	public Resolved<String> resolveTopic(@Nullable String userSpecifiedTopic, @Nullable Class<?> messageType,
			Supplier<String> defaultTopicSupplier) {
		var resolved = this.topicResolver.resolveTopic(userSpecifiedTopic, messageType, defaultTopicSupplier);
		return resolved.map(e -> this.expressionResolver.resolve(e).resolveToString().value().orElse(e));
	}

}
