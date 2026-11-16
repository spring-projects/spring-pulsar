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

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.jspecify.annotations.Nullable;

import org.springframework.pulsar.PulsarException;

/**
 * Pulsar {@link TableView} factory interface.
 *
 * @param <T> the type of the values of the table views created by this factory
 * @author Sharang Gupta
 * @since 2.1.0
 */
public interface PulsarTableViewFactory<T> {

	/**
	 * Create a Pulsar {@link TableView} of the given topic with the given schema.
	 * <p>
	 * The table view keeps reading the topic until it is closed, so the caller must close
	 * it when it is no longer needed.
	 * @param topic the topic to build the table view from, or null to use the topic of
	 * the default configuration of the factory
	 * @param schema the schema of the values
	 * @param customizers the optional list of customizers to apply to the table view
	 * builder. Note that the customizers are applied last and have the potential for
	 * overriding any specified parameters or default properties.
	 * @return the created table view
	 * @throws PulsarException if any {@link PulsarClientException} occurs communicating
	 * with Pulsar
	 */
	TableView<T> createTableView(@Nullable String topic, Schema<T> schema,
			@Nullable List<TableViewBuilderCustomizer<T>> customizers);

}
