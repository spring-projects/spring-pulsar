/*
 * Copyright 2023 the original author or authors.
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

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.Scope;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.Ordered;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.ConditionalGenericConverter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.log.LogAccessor;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.lang.Nullable;
import org.springframework.messaging.converter.GenericMessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

/**
 * Base class implementation for the various annotation post processors in Spring Pulsar.
 *
 * @author Soby Chacko
 */
public class AbstractPulsarAnnotationsBeanPostProcessor
		implements BeanPostProcessor, ApplicationContextAware, InitializingBean, Ordered {

	protected final LogAccessor logger = new LogAccessor(this.getClass());

	private static final String THE_LEFT = "The [";

	private static final String RESOLVED_TO_LEFT = "Resolved to [";

	private static final String RIGHT_FOR_LEFT = "] for [";

	protected BeanFactory beanFactory;

	protected ApplicationContext applicationContext;

	protected BeanExpressionResolver resolver;

	protected BeanExpressionContext expressionContext;

	protected final ListenerScope listenerScope = new ListenerScope();

	private Charset charset = StandardCharsets.UTF_8;

	protected final PulsarHandlerMethodFactoryAdapter messageHandlerMethodFactory = new PulsarHandlerMethodFactoryAdapter();

	@Override
	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		return bean;
	}

	protected void assertBeanFactory() {
		Assert.state(this.beanFactory != null, "BeanFactory must be set to obtain container factory by bean name");
	}

	public void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
		if (beanFactory instanceof ConfigurableListableBeanFactory) {
			this.resolver = ((ConfigurableListableBeanFactory) beanFactory).getBeanExpressionResolver();
			this.expressionContext = new BeanExpressionContext((ConfigurableListableBeanFactory) beanFactory,
					this.listenerScope);
		}
	}

	protected String noBeanFoundMessage(Object target, String listenerBeanName, String requestedBeanName,
			Class<?> expectedClass) {

		return "Could not register Pulsar listener endpoint on [" + target + "] for bean " + listenerBeanName + ", no '"
				+ expectedClass.getSimpleName() + "' with id '" + requestedBeanName
				+ "' was found in the application context";
	}

	protected Boolean resolveExpressionAsBoolean(String value, String attribute) {
		Object resolved = resolveExpression(value);
		Boolean result = null;
		if (resolved instanceof Boolean) {
			result = (Boolean) resolved;
		}
		else if (resolved instanceof String) {
			result = Boolean.parseBoolean((String) resolved);
		}
		else if (resolved != null) {
			throw new IllegalStateException(
					THE_LEFT + attribute + "] must resolve to a Boolean or a String that can be parsed as a Boolean. "
							+ RESOLVED_TO_LEFT + resolved.getClass() + RIGHT_FOR_LEFT + value + "]");
		}
		return result;
	}

	protected Object resolveExpression(String value) {
		return this.resolver.evaluate(resolve(value), this.expressionContext);
	}

	protected String resolve(String value) {
		if (this.beanFactory != null && this.beanFactory instanceof ConfigurableBeanFactory) {
			return ((ConfigurableBeanFactory) this.beanFactory).resolveEmbeddedValue(value);
		}
		return value;
	}

	protected String resolveExpressionAsString(String value, String attribute) {
		Object resolved = resolveExpression(value);
		if (resolved instanceof String) {
			return (String) resolved;
		}
		else if (resolved != null) {
			throw new IllegalStateException(THE_LEFT + attribute + "] must resolve to a String. " + RESOLVED_TO_LEFT
					+ resolved.getClass() + RIGHT_FOR_LEFT + value + "]");
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	protected void resolveAsString(Object resolvedValue, List<String> result) {
		if (resolvedValue instanceof String[]) {
			for (Object object : (String[]) resolvedValue) {
				resolveAsString(object, result);
			}
		}
		else if (resolvedValue instanceof String) {
			result.add((String) resolvedValue);
		}
		else if (resolvedValue instanceof Iterable) {
			for (Object object : (Iterable<Object>) resolvedValue) {
				resolveAsString(object, result);
			}
		}
		else {
			throw new IllegalArgumentException(
					"@PulsarListener can't resolve '%s' as a String".formatted(resolvedValue));
		}
	}

	protected Method checkProxy(Method methodArg, Object bean) {
		Method method = methodArg;
		if (AopUtils.isJdkDynamicProxy(bean)) {
			try {
				// Found a @PulsarListener method on the target class for this JDK proxy
				// ->
				// is it also present on the proxy itself?
				method = bean.getClass().getMethod(method.getName(), method.getParameterTypes());
				Class<?>[] proxiedInterfaces = ((Advised) bean).getProxiedInterfaces();
				for (Class<?> iface : proxiedInterfaces) {
					try {
						method = iface.getMethod(method.getName(), method.getParameterTypes());
						break;
					}
					catch (@SuppressWarnings("unused") NoSuchMethodException noMethod) {
						// NOSONAR
					}
				}
			}
			catch (SecurityException ex) {
				ReflectionUtils.handleReflectionException(ex);
			}
			catch (NoSuchMethodException ex) {
				throw new IllegalStateException("@PulsarListener method '%s' found on bean target class '%s', "
						+ "but not found in any interface(s) for bean JDK proxy. Either "
						+ "pull the method up to an interface or switch to subclass (CGLIB) "
						+ "proxies by setting proxy-target-class/proxyTargetClass "
						+ "attribute to 'true'".formatted(method.getName(), method.getDeclaringClass().getSimpleName()),
						ex);
			}
		}
		return method;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
		if (applicationContext instanceof ConfigurableApplicationContext) {
			setBeanFactory(((ConfigurableApplicationContext) applicationContext).getBeanFactory());
		}
		else {
			setBeanFactory(applicationContext);
		}
	}

	@Override
	public void afterPropertiesSet() {
		// place holder
	}

	@Override
	public int getOrder() {
		return LOWEST_PRECEDENCE;
	}

	protected void loadProperty(Properties properties, String property, Object value) {
		try {
			properties.load(new StringReader((String) value));
		}
		catch (IOException e) {
			this.logger.error(e, () -> "Failed to load property " + property + ", continuing...");
		}
	}

	protected Integer resolveExpressionAsInteger(String value, String attribute) {
		Object resolved = resolveExpression(value);
		Integer result = null;
		if (resolved instanceof String) {
			result = Integer.parseInt((String) resolved);
		}
		else if (resolved instanceof Number) {
			result = ((Number) resolved).intValue();
		}
		else if (resolved != null) {
			throw new IllegalStateException(
					THE_LEFT + attribute + "] must resolve to an Number or a String that can be parsed as an Integer. "
							+ RESOLVED_TO_LEFT + resolved.getClass() + RIGHT_FOR_LEFT + value + "]");
		}
		return result;
	}

	public static class ListenerScope implements Scope {

		private final Map<String, Object> listeners = new HashMap<>();

		public void addListener(String key, Object bean) {
			this.listeners.put(key, bean);
		}

		public void removeListener(String key) {
			this.listeners.remove(key);
		}

		@Override
		public Object get(String name, ObjectFactory<?> objectFactory) {
			return this.listeners.get(name);
		}

		@Override
		public Object remove(String name) {
			return null;
		}

		@Override
		public void registerDestructionCallback(String name, Runnable callback) {
		}

		@Override
		public Object resolveContextualObject(String key) {
			return this.listeners.get(key);
		}

		@Override
		public String getConversationId() {
			return null;
		}

	}

	protected class PulsarHandlerMethodFactoryAdapter implements MessageHandlerMethodFactory {

		protected final DefaultFormattingConversionService defaultFormattingConversionService = new DefaultFormattingConversionService();

		private MessageHandlerMethodFactory handlerMethodFactory;

		public void setHandlerMethodFactory(MessageHandlerMethodFactory pulsarHandlerMethodFactory1) {
			this.handlerMethodFactory = pulsarHandlerMethodFactory1;
		}

		@Override
		public InvocableHandlerMethod createInvocableHandlerMethod(Object bean, Method method) {
			return getHandlerMethodFactory().createInvocableHandlerMethod(bean, method);
		}

		private MessageHandlerMethodFactory getHandlerMethodFactory() {
			if (this.handlerMethodFactory == null) {
				this.handlerMethodFactory = createDefaultMessageHandlerMethodFactory();
			}
			return this.handlerMethodFactory;
		}

		private MessageHandlerMethodFactory createDefaultMessageHandlerMethodFactory() {
			DefaultMessageHandlerMethodFactory defaultFactory = new DefaultMessageHandlerMethodFactory();
			defaultFactory.setBeanFactory(AbstractPulsarAnnotationsBeanPostProcessor.this.beanFactory);
			this.defaultFormattingConversionService
				.addConverter(new BytesToStringConverter(AbstractPulsarAnnotationsBeanPostProcessor.this.charset));
			this.defaultFormattingConversionService.addConverter(new BytesToNumberConverter());
			defaultFactory.setConversionService(this.defaultFormattingConversionService);
			GenericMessageConverter messageConverter = new GenericMessageConverter(
					this.defaultFormattingConversionService);
			defaultFactory.setMessageConverter(messageConverter);

			defaultFactory.afterPropertiesSet();

			return defaultFactory;
		}

		public DefaultFormattingConversionService getDefaultFormattingConversionService() {
			return this.defaultFormattingConversionService;
		}

	}

	private static class BytesToStringConverter implements Converter<byte[], String> {

		private final Charset charset;

		BytesToStringConverter(Charset charset) {
			this.charset = charset;
		}

		@Override
		public String convert(byte[] source) {
			return new String(source, this.charset);
		}

	}

	private final class BytesToNumberConverter implements ConditionalGenericConverter {

		BytesToNumberConverter() {
		}

		@Override
		@Nullable
		public Set<ConvertiblePair> getConvertibleTypes() {
			HashSet<ConvertiblePair> pairs = new HashSet<>();
			pairs.add(new ConvertiblePair(byte[].class, long.class));
			pairs.add(new ConvertiblePair(byte[].class, int.class));
			pairs.add(new ConvertiblePair(byte[].class, short.class));
			pairs.add(new ConvertiblePair(byte[].class, byte.class));
			pairs.add(new ConvertiblePair(byte[].class, Long.class));
			pairs.add(new ConvertiblePair(byte[].class, Integer.class));
			pairs.add(new ConvertiblePair(byte[].class, Short.class));
			pairs.add(new ConvertiblePair(byte[].class, Byte.class));
			return pairs;
		}

		@Override
		@Nullable
		public Object convert(@Nullable Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {
			byte[] bytes = (byte[]) source;
			if (targetType.getType().equals(long.class) || targetType.getType().equals(Long.class)) {
				Assert.state(bytes.length >= 8, "At least 8 bytes needed to convert a byte[] to a long"); // NOSONAR
				return ByteBuffer.wrap(bytes).getLong();
			}
			else if (targetType.getType().equals(int.class) || targetType.getType().equals(Integer.class)) {
				Assert.state(bytes.length >= 4, "At least 4 bytes needed to convert a byte[] to an integer"); // NOSONAR
				return ByteBuffer.wrap(bytes).getInt();
			}
			else if (targetType.getType().equals(short.class) || targetType.getType().equals(Short.class)) {
				Assert.state(bytes.length >= 2, "At least 2 bytes needed to convert a byte[] to a short");
				return ByteBuffer.wrap(bytes).getShort();
			}
			else if (targetType.getType().equals(byte.class) || targetType.getType().equals(Byte.class)) {
				Assert.state(bytes.length >= 1, "At least 1 byte needed to convert a byte[] to a byte");
				return ByteBuffer.wrap(bytes).get();
			}
			return null;
		}

		@Override
		public boolean matches(TypeDescriptor sourceType, TypeDescriptor targetType) {
			if (sourceType.getType().equals(byte[].class)) {
				Class<?> target = targetType.getType();
				return target.equals(long.class) || target.equals(int.class) || target.equals(short.class) // NOSONAR
						|| target.equals(byte.class) || target.equals(Long.class) || target.equals(Integer.class)
						|| target.equals(Short.class) || target.equals(Byte.class);
			}
			return false;
		}

	}

}
