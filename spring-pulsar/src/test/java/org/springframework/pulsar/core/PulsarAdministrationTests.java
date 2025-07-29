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

package org.springframework.pulsar.core;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Unit tests for {@link PulsarAdministration}.
 *
 * @author Chris Bono
 */
public class PulsarAdministrationTests {

	private PulsarAdminBuilder adminBuilder = mock(PulsarAdminBuilder.class);

	@Nested
	class CustomizerTests {

		@Test
		void createdWithServiceUrlOnly() throws PulsarClientException {
			var admin = new PulsarAdministration("pulsar://foo:5150");
			admin.setAdminBuilder(adminBuilder);
			admin.createAdminClient();
			verify(adminBuilder).build();
		}

		@Test
		void createdWithNullCustomizer() throws PulsarClientException {
			PulsarAdminBuilderCustomizer customizer = null;
			var admin = new PulsarAdministration(customizer);
			admin.setAdminBuilder(adminBuilder);
			admin.createAdminClient();
			verify(adminBuilder).build();
		}

		@Test
		void createdWithSingleCustomizer() throws PulsarClientException {
			var customizer = mock(PulsarAdminBuilderCustomizer.class);
			var admin = new PulsarAdministration(customizer);
			admin.setAdminBuilder(adminBuilder);
			admin.createAdminClient();
			verify(customizer).customize(adminBuilder);
		}

		@Test
		void createdWithMultipleCustomizers() throws PulsarClientException {
			var customizer1 = mock(PulsarAdminBuilderCustomizer.class);
			var customizer2 = mock(PulsarAdminBuilderCustomizer.class);
			var admin = new PulsarAdministration(List.of(customizer2, customizer1));
			admin.setAdminBuilder(adminBuilder);
			admin.createAdminClient();
			InOrder inOrder = inOrder(customizer1, customizer2);
			inOrder.verify(customizer2).customize(adminBuilder);
			inOrder.verify(customizer1).customize(adminBuilder);
		}

	}

}
