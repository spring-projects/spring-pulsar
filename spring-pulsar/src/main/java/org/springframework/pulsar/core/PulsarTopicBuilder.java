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

package org.springframework.pulsar.core;

import org.apache.pulsar.common.naming.TopicDomain;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Builder class to create {@link PulsarTopic} instances.
 *
 * @author Alexander Preuß
 * @author Chris Bono
 */
public class PulsarTopicBuilder {

	private static final String FQ_TOPIC_NAME_FORMAT = "%s://%s/%s/%s";

	private static final String DEFAULT_TENANT = "public";

	private static final String DEFAULT_NAMESPACE = "default";

	private final TopicDomain defaultDomain;

	private final String defaultTenant;

	private final String defaultNamespace;

	@Nullable
	private String name;

	@Nullable
	private int numberOfPartitions;

	/**
	 * Create a builder instance that uses the following defaults. <pre>
	 * - {@code domain -> 'persistent'}
	 * - {@code tenant -> 'public'}
	 * - {@code namespace -> 'default'}
	 * </pre>
	 */
	public PulsarTopicBuilder() {
		this(TopicDomain.persistent, DEFAULT_TENANT, DEFAULT_NAMESPACE);
	}

	/**
	 * Create a builder instance that uses the specified defaults.
	 * @param defaultDomain domain to use when topic name is not fully-qualified
	 * @param defaultTenant tenant to use when topic name is not fully-qualified or null
	 * to use the Pulsar default tenant of 'public'
	 * @param defaultNamespace namespace to use when topic name is not fully-qualified or
	 * null to use the Pulsar default namespace of 'namespace'
	 */
	public PulsarTopicBuilder(TopicDomain defaultDomain, @Nullable String defaultTenant,
			@Nullable String defaultNamespace) {
		Assert.notNull(defaultDomain, "defaultDomain must not be null");
		this.defaultDomain = defaultDomain;
		this.defaultTenant = StringUtils.hasText(defaultTenant) ? defaultTenant : DEFAULT_TENANT;
		this.defaultNamespace = StringUtils.hasText(defaultNamespace) ? defaultNamespace : DEFAULT_NAMESPACE;
	}

	/**
	 * Get the fully-qualified name of the specified topic in the format
	 * {@code domain://tenant/namespace/name}.
	 * @param topicName the topic name to fully qualify
	 * @return the fully-qualified topic name
	 */
	public String getFullyQualifiedNameForTopic(String topicName) {
		return this.fullyQualifiedName(topicName);
	}

	/**
	 * Set the name of the topic under construction. The following formats are accepted:
	 * <pre>
	 * - {@code 'name'}
	 * - {@code 'tenant/namespace/name'}
	 * - {@code 'domain://tenant/namespace/name'}
	 * </pre> When the name is not fully-qualified the missing components are populated
	 * with the corresponding default configured on the builder.
	 * @param name the topic name
	 * @return this builder
	 */
	public PulsarTopicBuilder name(String name) {
		this.name = fullyQualifiedName(name);
		return this;
	}

	private String fullyQualifiedName(String name) {
		Assert.notNull(name, "name must not be null");
		String[] splitTopic = name.split("/");
		if (splitTopic.length == 1) { // e.g. 'my-topic'
			return FQ_TOPIC_NAME_FORMAT.formatted(this.defaultDomain, this.defaultTenant, this.defaultNamespace,
					splitTopic[0]);
		}
		if (splitTopic.length == 3) { // e.g. 'public/default/my-topic'
			return FQ_TOPIC_NAME_FORMAT.formatted(this.defaultDomain, splitTopic[0], splitTopic[1], splitTopic[2]);
		}
		if (splitTopic.length == 5) { // e.g. 'persistent://public/default/my-topic'
			String type = splitTopic[0].replace(":", "");
			return FQ_TOPIC_NAME_FORMAT.formatted(TopicDomain.getEnum(type), splitTopic[2], splitTopic[3],
					splitTopic[4]);
		}
		throw new IllegalArgumentException("Topic name '" + name + "' must be in one of "
				+ "the following formats ('name', 'tenant/namespace/name', 'domain://tenant/namespace/name')");
	}

	/**
	 * Sets the number of topic partitions for the topic under construction.
	 * @param numberOfPartitions the number of topic partitions
	 * @return this builder
	 */
	public PulsarTopicBuilder numberOfPartitions(int numberOfPartitions) {
		this.numberOfPartitions = numberOfPartitions;
		return this;
	}

	/**
	 * Constructs the {@link PulsarTopic} with the properties configured in this builder.
	 * @return {@link PulsarTopic}
	 */
	public PulsarTopic build() {
		return new PulsarTopic(this.name, this.numberOfPartitions);
	}

}
