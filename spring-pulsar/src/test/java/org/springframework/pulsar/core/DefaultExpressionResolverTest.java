/*
 * Copyright 2012-present the original author or authors.
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;

/**
 * Unit tests for {@link DefaultExpressionResolver}.
 *
 * @author Jonas Geiregat
 */
class DefaultExpressionResolverTest {

	@Test
	void resolveEvaluatedStringResult() {
		var configurableBeanFactory = mock(ConfigurableBeanFactory.class);
		var beanExpressionResolver = mock(BeanExpressionResolver.class);
		when(configurableBeanFactory.getBeanExpressionResolver()).thenReturn(beanExpressionResolver);
		when(configurableBeanFactory.resolveEmbeddedValue("${topic.name}")).thenReturn("resolved-topic-name");
		when(beanExpressionResolver.evaluate(eq("resolved-topic-name"), any(BeanExpressionContext.class)))
			.thenReturn("resolved-topic-name");

		ExpressionResolver expressionResolver = new DefaultExpressionResolver(configurableBeanFactory);
		Resolved<String> resolved = expressionResolver.resolveToString("${topic.name}");

		assertThat(resolved).isEqualTo(Resolved.of("resolved-topic-name"));
	}

	@Test
	void resolveEvaluatedNullResult() {
		var configurableBeanFactory = mock(ConfigurableBeanFactory.class);
		var beanExpressionResolver = mock(BeanExpressionResolver.class);
		when(configurableBeanFactory.getBeanExpressionResolver()).thenReturn(beanExpressionResolver);
		when(configurableBeanFactory.resolveEmbeddedValue("#{null")).thenReturn("#{null}");
		when(beanExpressionResolver.evaluate(eq("#{null}"), any(BeanExpressionContext.class))).thenReturn(null);

		ExpressionResolver expressionResolver = new DefaultExpressionResolver(configurableBeanFactory);
		Resolved<String> resolved = expressionResolver.resolveToString("#{null}");

		assertThat(resolved).isEqualTo(Resolved.of(null));
	}

	@Test
	void failToResolveEvaluatedNoneString() {
		var configurableBeanFactory = mock(ConfigurableBeanFactory.class);
		var beanExpressionResolver = mock(BeanExpressionResolver.class);
		when(configurableBeanFactory.getBeanExpressionResolver()).thenReturn(beanExpressionResolver);
		when(configurableBeanFactory.resolveEmbeddedValue("#{someBean.someProperty}"))
			.thenReturn("#{someBean.someProperty}");
		when(beanExpressionResolver.evaluate(eq("#{someBean.someProperty}"), any(BeanExpressionContext.class)))
			.thenReturn(new Object() {
			});

		ExpressionResolver expressionResolver = new DefaultExpressionResolver(configurableBeanFactory);
		Resolved<String> resolved = expressionResolver.resolveToString("#{someBean.someProperty}");

		assertThat(resolved.exception())
			.hasValueSatisfying((ex) -> assertThat(ex).isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("The expression '#{someBean.someProperty}' must resolve to a string but was: "));

	}

}
