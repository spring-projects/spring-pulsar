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

package org.springframework.pulsar.core;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.annotation.PulsarMessage;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Topic resolver that accepts custom type to topic mappings and uses the mappings during
 * topic resolution.
 * <p>
 * Message type to topic mappings can be configured with
 * {@link #addCustomTopicMapping(Class, String)}.
 *
 * @author Chris Bono
 * @author Aleksei Arsenev
 * @author Jonas Geiregat
 */
public class DefaultTopicResolver implements TopicResolver, BeanFactoryAware, BeanClassLoaderAware {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private final Map<String, String> customTopicMappings = new LinkedHashMap<>();

	private final PulsarMessageAnnotationRegistry pulsarMessageAnnotationRegistry = new PulsarMessageAnnotationRegistry();

	private boolean usePulsarMessageAnnotations = true;

	@Nullable
	private ExpressionResolver expressionResolver;

	@Nullable
	private ClassLoader classLoader;

	/**
	 * Constructs a new DefaultTopicResolver with the given expression resolver.
	 * @param expressionResolver the expression resolver to use for resolving topic
	 */
	public DefaultTopicResolver(ExpressionResolver expressionResolver) {
		this.expressionResolver = expressionResolver;
	}

	/**
	 * Constructs a new DefaultTopicResolver.
	 */
	public DefaultTopicResolver() {
	}

	/**
	 * Sets whether to inspect message classes for the
	 * {@link PulsarMessage @PulsarMessage} annotation during topic resolution.
	 * @param usePulsarMessageAnnotations whether to inspect messages for the annotation
	 */
	public void usePulsarMessageAnnotations(boolean usePulsarMessageAnnotations) {
		this.usePulsarMessageAnnotations = usePulsarMessageAnnotations;
	}

	/**
	 * Adds a custom mapping from message type to topic.
	 * @param messageType the message type
	 * @param topic the topic to use for messages of type {@code messageType}
	 * @return the previously mapped topic or {@code null} if there was no mapping for
	 * {@code messageType}.
	 */
	@Nullable
	public String addCustomTopicMapping(Class<?> messageType, String topic) {
		return this.customTopicMappings.put(this.toMessageTypeMapKey(messageType), topic);
	}

	/**
	 * Removes the custom mapping from message type to topic.
	 * @param messageType the message type
	 * @return the previously mapped topic or {@code null} if there was no mapping for
	 * {@code messageType}.
	 */
	@Nullable
	public String removeCustomMapping(Class<?> messageType) {
		return this.customTopicMappings.remove(this.toMessageTypeMapKey(messageType));
	}

	/**
	 * Gets the currently registered custom mappings from message type class name to
	 * topic.
	 * @return unmodifiable map of custom mappings
	 * @deprecated deprecated in favor of {@link #getCustomTopicMapping(Class)}
	 */
	@Deprecated(since = "1.2.5", forRemoval = true)
	public Map<Class<?>, String> getCustomTopicMappings() {
		return this.customTopicMappings.entrySet()
			.stream()
			.collect(Collectors.toMap((e) -> this.fromMessageTypeMapKey(e.getKey()), Entry::getValue));
	}

	/**
	 * Gets the currently registered custom mapping for the specified message type.
	 * @param messageType the message type
	 * @return optional custom topic registered for the message type
	 */
	public Optional<String> getCustomTopicMapping(Class<?> messageType) {
		return Optional.ofNullable(this.customTopicMappings.get(this.toMessageTypeMapKey(messageType)));
	}

	@Override
	public Resolved<String> resolveTopic(@Nullable String userSpecifiedTopic, Supplier<String> defaultTopicSupplier) {
		if (StringUtils.hasText(userSpecifiedTopic)) {
			return Resolved.of(userSpecifiedTopic);
		}
		String defaultTopic = defaultTopicSupplier.get();
		if (defaultTopic == null) {
			return Resolved.failed("Topic must be specified when no default topic is configured");
		}
		return Resolved.of(defaultTopic);
	}

	@Override
	public <T> Resolved<String> resolveTopic(@Nullable String userSpecifiedTopic, @Nullable T message,
			Supplier<String> defaultTopicSupplier) {
		return doResolveTopic(userSpecifiedTopic, message != null ? message.getClass() : null, defaultTopicSupplier);
	}

	@Override
	public Resolved<String> resolveTopic(@Nullable String userSpecifiedTopic, @Nullable Class<?> messageType,
			Supplier<String> defaultTopicSupplier) {
		return doResolveTopic(userSpecifiedTopic, messageType, defaultTopicSupplier);
	}

	protected Resolved<String> doResolveTopic(@Nullable String userSpecifiedTopic, @Nullable Class<?> messageType,
			Supplier<String> defaultTopicSupplier) {
		if (StringUtils.hasText(userSpecifiedTopic)) {
			return Resolved.of(userSpecifiedTopic);
		}
		if (messageType == null) {
			return Resolved.failed("Topic must be specified when the message is null");
		}
		// Check for custom topic mapping
		String topic = this.customTopicMappings.get(this.toMessageTypeMapKey(messageType));

		// If no custom topic mapping found, look for @PulsarMessage (if enabled)
		if (this.usePulsarMessageAnnotations && topic == null) {
			topic = getAnnotatedTopicInfo(messageType);
			if (topic != null) {
				this.addCustomTopicMapping(messageType, topic);
			}
		}

		// If still no topic, consult the default topic supplier
		if (topic == null) {
			topic = defaultTopicSupplier.get();
		}
		return topic == null ? Resolved.failed("Topic must be specified when no default topic is configured")
				: Resolved.of(topic);
	}

	// VisibleForTesting
	@Nullable
	String getAnnotatedTopicInfo(Class<?> messageType) {
		return this.pulsarMessageAnnotationRegistry.getAnnotationFor(messageType)
			.map(PulsarMessage::topic)
			.filter(StringUtils::hasText)
			.map(this::resolveExpression)
			.orElse(null);
	}

	private String resolveExpression(String v) {
		return this.expressionResolver == null ? v : this.expressionResolver.resolveToString(v)
			.orElseThrow(() -> "Failed to resolve topic expression: %s".formatted(v));
	}

	private Class<?> fromMessageTypeMapKey(String messageTypeKey) {
		try {
			return ClassUtils.forName(messageTypeKey, this.classLoader);
		}
		catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	private String toMessageTypeMapKey(Class<?> messageType) {
		return messageType.getName();
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		if (beanFactory instanceof ConfigurableBeanFactory configurableBeanFactory) {
			this.expressionResolver = new DefaultExpressionResolver(configurableBeanFactory);
		}
		else {
			this.logger.warn(
					() -> "Topic expressions on @PulsarMessage will not be resolved: bean factory must be %s but was %s"
						.formatted(ConfigurableBeanFactory.class.getSimpleName(),
								beanFactory.getClass().getSimpleName()));
		}
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

}
