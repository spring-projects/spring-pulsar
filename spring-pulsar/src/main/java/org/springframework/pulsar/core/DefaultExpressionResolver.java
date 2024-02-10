/*
 * Copyright 2012-2024 the original author or authors.
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

import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;

/**
 * Default implementation of {@link ExpressionResolver}.
 *
 * @author Jonas Geiregat
 */
public class DefaultExpressionResolver implements ExpressionResolver {

	private final BeanExpressionResolver beanExpressionResolver;

	private final BeanExpressionContext beanExpressionContext;

	private final ConfigurableListableBeanFactory configurableListableBeanFactory;

	public DefaultExpressionResolver(BeanExpressionResolver beanExpressionResolver,
			BeanExpressionContext beanExpressionContext,
			ConfigurableListableBeanFactory configurableListableBeanFactory) {
		this.beanExpressionResolver = beanExpressionResolver;
		this.beanExpressionContext = beanExpressionContext;
		this.configurableListableBeanFactory = configurableListableBeanFactory;
	}

	@Override
	public ResolvedExpression resolve(String expression) {
		return ResolvedExpression
			.of(this.beanExpressionResolver.evaluate(doResolve(expression), this.beanExpressionContext));
	}

	private String doResolve(String value) {
		return this.configurableListableBeanFactory.resolveEmbeddedValue(value);
	}

}
