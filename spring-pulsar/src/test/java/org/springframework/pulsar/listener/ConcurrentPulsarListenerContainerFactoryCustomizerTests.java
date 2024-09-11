/*
 * Copyright 2023-2024 the original author or authors.
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

package org.springframework.pulsar.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.pulsar.config.ConcurrentPulsarListenerContainerFactory;
import org.springframework.pulsar.config.ConcurrentPulsarListenerContainerFactoryCustomizer;

/**
 * Tests for applying {@link ConcurrentPulsarListenerContainerFactoryCustomizer} to the
 * {@link ConcurrentPulsarListenerContainerFactory}.
 *
 * @author Chris Bono
 */
@SuppressWarnings("removal")
class ConcurrentPulsarListenerContainerFactoryCustomizerTests {

	@Test
	void whenSingleCustomizerAvailableThenItIsApplied() {
		var containerFactory = mock(ConcurrentPulsarListenerContainerFactory.class);
		var containerProps = new PulsarContainerProperties();
		when(containerFactory.getContainerProperties()).thenReturn(containerProps);
		ConcurrentPulsarListenerContainerFactoryCustomizer<?> customizer = (
				cf) -> cf.getContainerProperties().transactions().setTimeout(Duration.ofSeconds(45));
		try (var appContext = new AnnotationConfigApplicationContext()) {
			appContext.registerBean(ConcurrentPulsarListenerContainerFactory.class, () -> containerFactory);
			appContext.registerBean(ConcurrentPulsarListenerContainerFactoryCustomizer.class, () -> customizer);
			appContext.register(ConcurrentPulsarListenerContainerFactoryCustomizerTestsConfig.class);
			appContext.refresh();
			assertThat(containerProps.transactions().getTimeout()).isEqualTo(Duration.ofSeconds(45));
		}
	}

	@Test
	void whenMultipleCustomizersAvailableThenNoneAreApplied() {
		var containerFactory = mock(ConcurrentPulsarListenerContainerFactory.class);
		var containerProps = new PulsarContainerProperties();
		when(containerFactory.getContainerProperties()).thenReturn(containerProps);
		ConcurrentPulsarListenerContainerFactoryCustomizer<?> customizer1 = (
				cf) -> cf.getContainerProperties().transactions().setTimeout(Duration.ofSeconds(45));
		ConcurrentPulsarListenerContainerFactoryCustomizer<?> customizer2 = (
				cf) -> cf.getContainerProperties().transactions().setTimeout(Duration.ofSeconds(60));
		try (var appContext = new AnnotationConfigApplicationContext()) {
			appContext.registerBean(ConcurrentPulsarListenerContainerFactory.class, () -> containerFactory);
			appContext.registerBean("customizer1", ConcurrentPulsarListenerContainerFactoryCustomizer.class,
					() -> customizer1);
			appContext.registerBean("customizer2", ConcurrentPulsarListenerContainerFactoryCustomizer.class,
					() -> customizer2);
			appContext.register(ConcurrentPulsarListenerContainerFactoryCustomizerTestsConfig.class);
			appContext.refresh();
			assertThat(containerProps.transactions().getTimeout()).isNull();
		}
	}

	@Test
	void whenNoCustomizersAvaiableThenContextStartsWithoutFailure() {
		var containerFactory = mock(ConcurrentPulsarListenerContainerFactory.class);
		try (var appContext = new AnnotationConfigApplicationContext()) {
			appContext.registerBean(ConcurrentPulsarListenerContainerFactory.class, () -> containerFactory);
			appContext.register(ConcurrentPulsarListenerContainerFactoryCustomizerTestsConfig.class);
			appContext.refresh();
		}
	}

	@Configuration(proxyBeanMethods = false)
	@EnablePulsar
	static class ConcurrentPulsarListenerContainerFactoryCustomizerTestsConfig {

	}

}
