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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.client.api.TableViewBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import org.springframework.pulsar.PulsarException;

/**
 * Unit tests for {@link DefaultPulsarTableViewFactory} with a mocked
 * {@link PulsarClient}.
 *
 * @author Sharang Gupta
 */
class DefaultPulsarTableViewFactoryUnitTests {

	private PulsarClient mockClient;

	private TableViewBuilder<String> mockBuilder;

	@BeforeEach
	@SuppressWarnings("unchecked")
	void prepareMocks() throws Exception {
		this.mockClient = mock(PulsarClient.class);
		this.mockBuilder = mock(TableViewBuilder.class);
		given(this.mockClient.newTableView(Schema.STRING)).willReturn(this.mockBuilder);
		given(this.mockBuilder.topic(anyString())).willReturn(this.mockBuilder);
		given(this.mockBuilder.subscriptionName(anyString())).willReturn(this.mockBuilder);
		given(this.mockBuilder.create()).willReturn(mock(TableView.class));
	}

	@Test
	void defaultsThenTopicThenCustomizersAreApplied() throws Exception {
		var tableViewFactory = new DefaultPulsarTableViewFactory<String>(this.mockClient,
				List.<TableViewBuilderCustomizer<String>>of((builder) -> builder.subscriptionName("default-sub")));
		tableViewFactory.setTopicBuilder(new PulsarTopicBuilder());

		tableViewFactory.createTableView("my-topic", Schema.STRING,
				List.<TableViewBuilderCustomizer<String>>of((builder) -> builder.subscriptionName("specific-sub")));

		InOrder inOrder = inOrder(this.mockBuilder);
		inOrder.verify(this.mockBuilder).subscriptionName("default-sub");
		inOrder.verify(this.mockBuilder).topic("persistent://public/default/my-topic");
		inOrder.verify(this.mockBuilder).subscriptionName("specific-sub");
		inOrder.verify(this.mockBuilder).create();
	}

	@Test
	void topicIsNotSetWhenNotSpecified() throws Exception {
		var tableViewFactory = new DefaultPulsarTableViewFactory<String>(this.mockClient);

		tableViewFactory.createTableView(null, Schema.STRING, null);

		verify(this.mockBuilder, never()).topic(any());
		verify(this.mockBuilder).create();
	}

	@Test
	void pulsarClientExceptionIsWrapped() throws Exception {
		var tableViewFactory = new DefaultPulsarTableViewFactory<String>(this.mockClient);
		var cause = new PulsarClientException("boom");
		given(this.mockBuilder.create()).willThrow(cause);

		assertThatThrownBy(() -> tableViewFactory.createTableView("my-topic", Schema.STRING, null))
			.isInstanceOf(PulsarException.class)
			.hasCause(cause);
	}

	@Test
	void schemaIsRequired() {
		var tableViewFactory = new DefaultPulsarTableViewFactory<String>(this.mockClient);

		assertThatThrownBy(() -> tableViewFactory.createTableView("my-topic", null, null))
			.isInstanceOf(NullPointerException.class)
			.hasMessage("Schema must be specified");
	}

}
