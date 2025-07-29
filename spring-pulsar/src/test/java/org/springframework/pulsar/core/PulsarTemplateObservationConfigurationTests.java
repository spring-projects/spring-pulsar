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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import org.springframework.context.support.GenericApplicationContext;
import org.springframework.pulsar.observation.PulsarTemplateObservationConvention;

import io.micrometer.observation.ObservationRegistry;

/**
 * Tests for the observation configuration aspect of {@link PulsarTemplate}.
 *
 * @author Chris Bono
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
class PulsarTemplateObservationConfigurationTests {

	@Test
	void uniqueRegistryAndConventionBeansAvailable() {
		var template = new PulsarTemplate(mock(PulsarProducerFactory.class), Collections.emptyList(),
				new DefaultSchemaResolver(), new DefaultTopicResolver(), true);
		var appContext = new GenericApplicationContext();
		var observationRegistry = mock(ObservationRegistry.class);
		var observationConvention = mock(PulsarTemplateObservationConvention.class);
		appContext.registerBean("obsReg", ObservationRegistry.class, () -> observationRegistry);
		appContext.registerBean("obsConv", PulsarTemplateObservationConvention.class, () -> observationConvention);
		appContext.refresh();
		template.setApplicationContext(appContext);
		template.afterSingletonsInstantiated();
		assertThat(template).hasFieldOrPropertyWithValue("observationEnabled", true);
		assertThat(template).hasFieldOrPropertyWithValue("observationRegistry", observationRegistry);
		assertThat(template).hasFieldOrPropertyWithValue("observationConvention", observationConvention);
	}

	@Test
	void enabledPropertySetToFalse() {
		var template = new PulsarTemplate(mock(PulsarProducerFactory.class), Collections.emptyList(),
				new DefaultSchemaResolver(), new DefaultTopicResolver(), false);
		var appContext = new GenericApplicationContext();
		var observationRegistry = mock(ObservationRegistry.class);
		var observationConvention = mock(PulsarTemplateObservationConvention.class);
		appContext.registerBean("obsReg", ObservationRegistry.class, () -> observationRegistry);
		appContext.registerBean("obsConv", PulsarTemplateObservationConvention.class, () -> observationConvention);
		appContext.refresh();
		template.setApplicationContext(appContext);
		template.afterSingletonsInstantiated();
		assertThat(template).hasFieldOrPropertyWithValue("observationEnabled", false);
		assertThat(template).hasFieldOrPropertyWithValue("observationRegistry", null);
		assertThat(template).hasFieldOrPropertyWithValue("observationConvention", null);
	}

	@Test
	void noAppContextAvailable() {
		var template = new PulsarTemplate(mock(PulsarProducerFactory.class), Collections.emptyList(),
				new DefaultSchemaResolver(), new DefaultTopicResolver(), true);
		template.afterSingletonsInstantiated();
		assertThat(template).hasFieldOrPropertyWithValue("observationEnabled", true);
		assertThat(template).hasFieldOrPropertyWithValue("observationRegistry", null);
		assertThat(template).hasFieldOrPropertyWithValue("observationConvention", null);
	}

	@Test
	void noRegistryOrConventionBeansAvailable() {
		var template = new PulsarTemplate(mock(PulsarProducerFactory.class), Collections.emptyList(),
				new DefaultSchemaResolver(), new DefaultTopicResolver(), true);
		var appContext = new GenericApplicationContext();
		appContext.refresh();
		template.setApplicationContext(appContext);
		template.afterSingletonsInstantiated();
		assertThat(template).hasFieldOrPropertyWithValue("observationEnabled", true);
		assertThat(template).hasFieldOrPropertyWithValue("observationRegistry", null);
		assertThat(template).hasFieldOrPropertyWithValue("observationConvention", null);
	}

	@Test
	void noUniqueRegistryOrConventionBeansAvailable() {
		var template = new PulsarTemplate(mock(PulsarProducerFactory.class), Collections.emptyList(),
				new DefaultSchemaResolver(), new DefaultTopicResolver(), true);
		var appContext = new GenericApplicationContext();
		var observationRegistry = mock(ObservationRegistry.class);
		var observationConvention = mock(PulsarTemplateObservationConvention.class);
		appContext.registerBean("obsReg1", ObservationRegistry.class, () -> observationRegistry);
		appContext.registerBean("obsReg2", ObservationRegistry.class, () -> observationRegistry);
		appContext.registerBean("obsConv1", PulsarTemplateObservationConvention.class, () -> observationConvention);
		appContext.registerBean("obsConv2", PulsarTemplateObservationConvention.class, () -> observationConvention);
		appContext.refresh();
		template.setApplicationContext(appContext);
		template.afterSingletonsInstantiated();
		assertThat(template).hasFieldOrPropertyWithValue("observationEnabled", true);
		assertThat(template).hasFieldOrPropertyWithValue("observationRegistry", null);
		assertThat(template).hasFieldOrPropertyWithValue("observationConvention", null);
	}

}
