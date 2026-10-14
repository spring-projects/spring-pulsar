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

import org.apache.pulsar.client.api.TableViewBuilder;

/**
 * Callback interface that can be implemented to customize the {@link TableViewBuilder}
 * that is used by the {@link PulsarTableViewFactory} to create table views.
 * <p>
 * The table view factory applies the customizers of its default configuration to every
 * created table view, and the customizers passed in when
 * {@link PulsarTableViewFactory#createTableView creating a table view} to that single
 * table view.
 *
 * @param <T> the type of the values of the table view
 * @author Sharang Gupta
 * @since 2.1.0
 */
@FunctionalInterface
public interface TableViewBuilderCustomizer<T> {

	/**
	 * Customizes a {@link TableViewBuilder}.
	 * @param tableViewBuilder the builder to customize
	 */
	void customize(TableViewBuilder<T> tableViewBuilder);

}
