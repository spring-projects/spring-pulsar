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

package org.springframework.pulsar.annotation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Tests for {@link PulsarHeaderObjectMapperUtils}.
 */
class PulsarHeaderObjectMapperUtilsTests {

	@Test
	void whenCustomMapperDefinedItIsReturned() {
		var mapper = new ObjectMapper();
		var beanFactory = mock(BeanFactory.class);
		when(beanFactory.getBean("pulsarHeaderObjectMapper", ObjectMapper.class)).thenReturn(mapper);
		assertThat(PulsarHeaderObjectMapperUtils.customMapper(beanFactory)).hasValue(mapper);
	}

	@Test
	void whenCustomMapperIsNotDefinedEmptyIsReturned() {
		var beanFactory = mock(BeanFactory.class);
		when(beanFactory.getBean("pulsarHeaderObjectMapper", ObjectMapper.class))
			.thenThrow(new NoSuchBeanDefinitionException("pulsarHeaderObjectMapper"));
		assertThat(PulsarHeaderObjectMapperUtils.customMapper(beanFactory)).isEmpty();
	}

	@Test
	void whenBeanFactoryNullExceptionIsThrown() {
		assertThatIllegalArgumentException().isThrownBy(() -> PulsarHeaderObjectMapperUtils.customMapper(null))
			.withMessage("beanFactory must not be null");
	}

}
