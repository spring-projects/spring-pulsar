/*
 * Copyright 2026-present the original author or authors.
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

import java.util.List;
import java.util.Objects;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.client.api.TableViewBuilder;
import org.jspecify.annotations.Nullable;

import org.springframework.pulsar.PulsarException;
import org.springframework.util.CollectionUtils;

/**
 * Default implementation of {@link PulsarTableViewFactory}.
 *
 * @param <T> the type of the values of the table views created by this factory
 * @author Sharang Gupta
 * @since 2.1.0
 */
public class DefaultPulsarTableViewFactory<T> implements PulsarTableViewFactory<T> {

	private final PulsarClient pulsarClient;

	private @Nullable final List<TableViewBuilderCustomizer<T>> defaultConfigCustomizers;

	private @Nullable PulsarTopicBuilder topicBuilder;

	/**
	 * Construct a table view factory instance with no default configuration.
	 * @param pulsarClient the client used to create the table views
	 */
	public DefaultPulsarTableViewFactory(PulsarClient pulsarClient) {
		this(pulsarClient, null);
	}

	/**
	 * Construct a table view factory instance.
	 * @param pulsarClient the client used to create the table views
	 * @param defaultConfigCustomizers the optional list of customizers to apply to the
	 * table views or null to use no default configuration
	 */
	public DefaultPulsarTableViewFactory(PulsarClient pulsarClient,
			@Nullable List<TableViewBuilderCustomizer<T>> defaultConfigCustomizers) {
		this.pulsarClient = pulsarClient;
		this.defaultConfigCustomizers = defaultConfigCustomizers;
	}

	/**
	 * Non-fully-qualified topic names specified on the created table views will be
	 * automatically fully-qualified with a default prefix
	 * ({@code domain://tenant/namespace}) according to the specified topic builder.
	 * @param topicBuilder the topic builder used to fully qualify topic names or null to
	 * not fully qualify topic names
	 */
	public void setTopicBuilder(@Nullable PulsarTopicBuilder topicBuilder) {
		this.topicBuilder = topicBuilder;
	}

	@Override
	public TableView<T> createTableView(@Nullable String topic, Schema<T> schema,
			@Nullable List<TableViewBuilderCustomizer<T>> customizers) {
		Objects.requireNonNull(schema, "Schema must be specified");
		TableViewBuilder<T> tableViewBuilder = this.pulsarClient.newTableView(schema);
		if (!CollectionUtils.isEmpty(this.defaultConfigCustomizers)) {
			this.defaultConfigCustomizers.forEach((customizer) -> customizer.customize(tableViewBuilder));
		}
		if (topic != null) {
			tableViewBuilder.topic(fullyQualifiedTopicName(topic));
		}
		if (!CollectionUtils.isEmpty(customizers)) {
			customizers.forEach((customizer) -> customizer.customize(tableViewBuilder));
		}
		try {
			return tableViewBuilder.create();
		}
		catch (PulsarClientException ex) {
			throw new PulsarException(ex);
		}
	}

	private String fullyQualifiedTopicName(String topic) {
		return (this.topicBuilder != null) ? this.topicBuilder.getFullyQualifiedNameForTopic(topic) : topic;
	}

}
