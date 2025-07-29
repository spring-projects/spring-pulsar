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

import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;

/**
 * Default implementation of {@link ExpressionResolver} that relies on the
 * {@link ConfigurableBeanFactory} capabilities to resolve expressions.
 *
 * @author Jonas Geiregat
 * @since 1.1.0
 */
public class DefaultExpressionResolver implements ExpressionResolver {

	private final BeanExpressionResolver beanExpressionResolver;

	private final BeanExpressionContext beanExpressionContext;

	private final ConfigurableBeanFactory configurableBeanFactory;

	public DefaultExpressionResolver(ConfigurableBeanFactory configurableBeanFactory) {
		this.beanExpressionResolver = configurableBeanFactory.getBeanExpressionResolver();
		this.beanExpressionContext = new BeanExpressionContext(configurableBeanFactory, null);
		this.configurableBeanFactory = configurableBeanFactory;
	}

	/**
	 * {@inheritDoc}
	 * @param expression the expression to resolve (can include property placeholders and
	 * SpEL)
	 * @return a {@code Resolved} instance containing the resolved string value (can be
	 * null) or an exception if the resolution failed.
	 */
	@Override
	public Resolved<String> resolveToString(String expression) {
		String placeholdersResolved = this.configurableBeanFactory.resolveEmbeddedValue(expression);
		Object resolvedObj = this.beanExpressionResolver.evaluate(placeholdersResolved, this.beanExpressionContext);
		if (resolvedObj instanceof String resolvedString) {
			return Resolved.of(resolvedString);
		}
		if (resolvedObj == null) {
			return Resolved.of(null);
		}
		return Resolved
			.failed("The expression '%s' must resolve to a string but was: %s".formatted(expression, resolvedObj));
	}

}
