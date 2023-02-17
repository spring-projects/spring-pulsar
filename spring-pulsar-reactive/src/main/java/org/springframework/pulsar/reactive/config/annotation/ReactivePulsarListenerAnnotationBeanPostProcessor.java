/*
 * Copyright 2022-2023 the original author or authors.
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

package org.springframework.pulsar.reactive.config.annotation;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import org.apache.pulsar.client.api.DeadLetterPolicy;

import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.Scope;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.ConditionalGenericConverter;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.core.log.LogAccessor;
import org.springframework.format.Formatter;
import org.springframework.format.FormatterRegistry;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.lang.Nullable;
import org.springframework.messaging.converter.GenericMessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.pulsar.annotation.PulsarListenerConfigurer;
import org.springframework.pulsar.config.PulsarAnnotationSupportBeanNames;
import org.springframework.pulsar.config.PulsarListenerEndpointRegistrar;
import org.springframework.pulsar.reactive.config.MethodReactivePulsarListenerEndpoint;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerContainerFactory;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerEndpoint;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerEndpointRegistry;
import org.springframework.pulsar.reactive.core.ReactiveMessageConsumerBuilderCustomizer;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * Bean post-processor that registers methods annotated with
 * {@link ReactivePulsarListener} to be invoked by a Pulsar message listener container
 * created under the covers by a {@link ReactivePulsarListenerContainerFactory} according
 * to the parameters of the annotation.
 *
 * <p>
 * Annotated methods can use flexible arguments as defined by
 * {@link ReactivePulsarListener}.
 *
 * <p>
 * This post-processor is automatically registered by the {@link EnableReactivePulsar}
 * annotation.
 *
 * <p>
 * Auto-detect any {@link PulsarListenerConfigurer} instances in the container, allowing
 * for customization of the registry to be used, the default container factory or for
 * fine-grained control over endpoints registration. See {@link EnableReactivePulsar}
 * Javadoc for complete usage details.
 *
 * @param <V> the payload type.
 * @author Christophe Bornet
 * @see ReactivePulsarListener
 * @see EnableReactivePulsar
 * @see PulsarListenerConfigurer
 * @see PulsarListenerEndpointRegistrar
 * @see ReactivePulsarListenerEndpointRegistry
 * @see ReactivePulsarListenerEndpoint
 * @see MethodReactivePulsarListenerEndpoint
 */
public class ReactivePulsarListenerAnnotationBeanPostProcessor<V>
		implements BeanPostProcessor, Ordered, ApplicationContextAware, InitializingBean, SmartInitializingSingleton {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	/**
	 * The bean name of the default {@link ReactivePulsarListenerContainerFactory}.
	 */
	public static final String DEFAULT_REACTIVE_PULSAR_LISTENER_CONTAINER_FACTORY_BEAN_NAME = "reactivePulsarListenerContainerFactory";

	private static final String THE_LEFT = "The [";

	private static final String RESOLVED_TO_LEFT = "Resolved to [";

	private static final String RIGHT_FOR_LEFT = "] for [";

	private static final String GENERATED_ID_PREFIX = "org.springframework.Pulsar.ReactivePulsarListenerEndpointContainer#";

	private ApplicationContext applicationContext;

	private BeanFactory beanFactory;

	private BeanExpressionResolver resolver;

	private BeanExpressionContext expressionContext;

	private ReactivePulsarListenerEndpointRegistry<?> endpointRegistry;

	private String defaultContainerFactoryBeanName = DEFAULT_REACTIVE_PULSAR_LISTENER_CONTAINER_FACTORY_BEAN_NAME;

	private final PulsarListenerEndpointRegistrar registrar = new PulsarListenerEndpointRegistrar(
			ReactivePulsarListenerContainerFactory.class);

	private final PulsarHandlerMethodFactoryAdapter messageHandlerMethodFactory = new PulsarHandlerMethodFactoryAdapter();

	private Charset charset = StandardCharsets.UTF_8;

	private final Set<Class<?>> nonAnnotatedClasses = Collections.newSetFromMap(new ConcurrentHashMap<>(64));

	private final ListenerScope listenerScope = new ListenerScope();

	private AnnotationEnhancer enhancer;

	private final AtomicInteger counter = new AtomicInteger();

	@Override
	public int getOrder() {
		return LOWEST_PRECEDENCE;
	}

	public void setEndpointRegistry(ReactivePulsarListenerEndpointRegistry<?> endpointRegistry) {
		this.endpointRegistry = endpointRegistry;
	}

	public void setDefaultContainerFactoryBeanName(String containerFactoryBeanName) {
		this.defaultContainerFactoryBeanName = containerFactoryBeanName;
	}

	public void setCharset(Charset charset) {
		Assert.notNull(charset, "'charset' cannot be null");
		this.charset = charset;
	}

	@Override
	public void afterPropertiesSet() {
		buildEnhancer();
	}

	private void buildEnhancer() {
		if (this.applicationContext != null) {
			List<AnnotationEnhancer> enhancers = this.applicationContext
					.getBeanProvider(AnnotationEnhancer.class, false).orderedStream().toList();
			if (!enhancers.isEmpty()) {
				this.enhancer = (attrs, element) -> {
					for (AnnotationEnhancer enh : enhancers) {
						attrs = enh.apply(attrs, element);
					}
					return attrs;
				};
			}
		}
	}

	@Override
	public void afterSingletonsInstantiated() {
		this.registrar.setBeanFactory(this.beanFactory);

		this.beanFactory.getBeanProvider(PulsarListenerConfigurer.class)
				.forEach(c -> c.configurePulsarListeners(this.registrar));

		if (this.registrar.getEndpointRegistry() == null) {
			if (this.endpointRegistry == null) {
				Assert.state(this.beanFactory != null,
						"BeanFactory must be set to find endpoint registry by bean name");
				this.endpointRegistry = this.beanFactory.getBean(
						PulsarAnnotationSupportBeanNames.REACTIVE_PULSAR_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME,
						ReactivePulsarListenerEndpointRegistry.class);
			}
			this.registrar.setEndpointRegistry(this.endpointRegistry);
		}

		if (this.defaultContainerFactoryBeanName != null) {
			this.registrar.setContainerFactoryBeanName(this.defaultContainerFactoryBeanName);
		}
		addFormatters(this.messageHandlerMethodFactory.defaultFormattingConversionService);

		// Actually register all listeners
		this.registrar.afterPropertiesSet();
	}

	@Override
	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		return bean;
	}

	@Override
	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
		if (!this.nonAnnotatedClasses.contains(bean.getClass())) {
			Class<?> targetClass = AopUtils.getTargetClass(bean);
			Map<Method, Set<ReactivePulsarListener>> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
					(MethodIntrospector.MetadataLookup<Set<ReactivePulsarListener>>) method -> {
						Set<ReactivePulsarListener> listenerMethods = findListenerAnnotations(method);
						return (!listenerMethods.isEmpty() ? listenerMethods : null);
					});
			if (annotatedMethods.isEmpty()) {
				this.nonAnnotatedClasses.add(bean.getClass());
				this.logger.trace(() -> "No @PulsarListener annotations found on bean type: " + bean.getClass());
			}
			else {
				// Non-empty set of methods
				for (Map.Entry<Method, Set<ReactivePulsarListener>> entry : annotatedMethods.entrySet()) {
					Method method = entry.getKey();
					for (ReactivePulsarListener listener : entry.getValue()) {
						processReactivePulsarListener(listener, method, bean, beanName);
					}
				}
				this.logger.debug(() -> annotatedMethods.size() + " @ReactivePulsarListener methods processed on bean '"
						+ beanName + "': " + annotatedMethods);
			}
		}
		return bean;
	}

	protected void processReactivePulsarListener(ReactivePulsarListener reactivePulsarListener, Method method,
			Object bean, String beanName) {
		Method methodToUse = checkProxy(method, bean);
		MethodReactivePulsarListenerEndpoint<V> endpoint = new MethodReactivePulsarListenerEndpoint<>();
		endpoint.setMethod(methodToUse);

		String beanRef = reactivePulsarListener.beanRef();
		this.listenerScope.addListener(beanRef, bean);
		String[] topics = resolveTopics(reactivePulsarListener);
		String topicPattern = getTopicPattern(reactivePulsarListener);
		processListener(endpoint, reactivePulsarListener, bean, beanName, topics, topicPattern);
		this.listenerScope.removeListener(beanRef);
	}

	protected void processListener(MethodReactivePulsarListenerEndpoint<?> endpoint,
			ReactivePulsarListener ReactivePulsarListener, Object bean, String beanName, String[] topics,
			String topicPattern) {

		processReactivePulsarListenerAnnotation(endpoint, ReactivePulsarListener, bean, topics, topicPattern);

		String containerFactory = resolve(ReactivePulsarListener.containerFactory());
		ReactivePulsarListenerContainerFactory<?> listenerContainerFactory = resolveContainerFactory(
				ReactivePulsarListener, containerFactory, beanName);

		this.registrar.registerEndpoint(endpoint, listenerContainerFactory);
	}

	@Nullable
	private ReactivePulsarListenerContainerFactory<?> resolveContainerFactory(
			ReactivePulsarListener ReactivePulsarListener, Object factoryTarget, String beanName) {

		String containerFactory = ReactivePulsarListener.containerFactory();
		if (!StringUtils.hasText(containerFactory)) {
			return null;
		}

		ReactivePulsarListenerContainerFactory<?> factory = null;

		Object resolved = resolveExpression(containerFactory);
		if (resolved instanceof ReactivePulsarListenerContainerFactory) {
			return (ReactivePulsarListenerContainerFactory<?>) resolved;
		}
		String containerFactoryBeanName = resolveExpressionAsString(containerFactory, "containerFactory");
		if (StringUtils.hasText(containerFactoryBeanName)) {
			assertBeanFactory();
			try {
				factory = this.beanFactory.getBean(containerFactoryBeanName,
						ReactivePulsarListenerContainerFactory.class);
			}
			catch (NoSuchBeanDefinitionException ex) {
				throw new BeanInitializationException(noBeanFoundMessage(factoryTarget, beanName,
						containerFactoryBeanName, ReactivePulsarListenerContainerFactory.class), ex);
			}
		}
		return factory;
	}

	protected void assertBeanFactory() {
		Assert.state(this.beanFactory != null, "BeanFactory must be set to obtain container factory by bean name");
	}

	protected String noBeanFoundMessage(Object target, String listenerBeanName, String requestedBeanName,
			Class<?> expectedClass) {

		return "Could not register Pulsar listener endpoint on [" + target + "] for bean " + listenerBeanName + ", no '"
				+ expectedClass.getSimpleName() + "' with id '" + requestedBeanName
				+ "' was found in the application context";
	}

	private void processReactivePulsarListenerAnnotation(MethodReactivePulsarListenerEndpoint<?> endpoint,
			ReactivePulsarListener reactivePulsarListener, Object bean, String[] topics, String topicPattern) {

		endpoint.setBean(bean);
		endpoint.setMessageHandlerMethodFactory(this.messageHandlerMethodFactory);
		endpoint.setSubscriptionName(getEndpointSubscriptionName(reactivePulsarListener));
		endpoint.setId(getEndpointId(reactivePulsarListener));
		endpoint.setTopics(topics);
		endpoint.setTopicPattern(topicPattern);
		endpoint.setSubscriptionType(reactivePulsarListener.subscriptionType());
		endpoint.setSchemaType(reactivePulsarListener.schemaType());

		String concurrency = reactivePulsarListener.concurrency();
		if (StringUtils.hasText(concurrency)) {
			endpoint.setConcurrency(resolveExpressionAsInteger(concurrency, "concurrency"));
		}
		String useKeyOrderedProcessing = reactivePulsarListener.useKeyOrderedProcessing();
		if (StringUtils.hasText(useKeyOrderedProcessing)) {
			endpoint.setUseKeyOrderedProcessing(
					resolveExpressionAsBoolean(useKeyOrderedProcessing, "useKeyOrderedProcessing"));
		}

		String autoStartup = reactivePulsarListener.autoStartup();
		if (StringUtils.hasText(autoStartup)) {
			endpoint.setAutoStartup(resolveExpressionAsBoolean(autoStartup, "autoStartup"));
		}
		endpoint.setFluxListener(reactivePulsarListener.stream());
		endpoint.setBeanFactory(this.beanFactory);

		resolveDeadLetterPolicy(endpoint, reactivePulsarListener);
		resolveConsumerCustomizer(endpoint, reactivePulsarListener);
	}

	private void resolveDeadLetterPolicy(MethodReactivePulsarListenerEndpoint<?> endpoint,
			ReactivePulsarListener reactivePulsarListener) {
		Object deadLetterPolicy = resolveExpression(reactivePulsarListener.deadLetterPolicy());
		if (deadLetterPolicy instanceof DeadLetterPolicy) {
			endpoint.setDeadLetterPolicy((DeadLetterPolicy) deadLetterPolicy);
		}
		else {
			String deadLetterPolicyBeanName = resolveExpressionAsString(reactivePulsarListener.deadLetterPolicy(),
					"deadLetterPolicy");
			if (StringUtils.hasText(deadLetterPolicyBeanName)) {
				endpoint.setDeadLetterPolicy(
						this.beanFactory.getBean(deadLetterPolicyBeanName, DeadLetterPolicy.class));
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void resolveConsumerCustomizer(MethodReactivePulsarListenerEndpoint<?> endpoint,
			ReactivePulsarListener reactivePulsarListener) {
		Object customizer = resolveExpression(reactivePulsarListener.consumerCustomizer());
		if (customizer instanceof ReactiveMessageConsumerBuilderCustomizer<?>) {
			endpoint.setConsumerCustomizer((ReactiveMessageConsumerBuilderCustomizer) customizer);
		}
		else {
			String consumerCustomizerBeanName = resolveExpressionAsString(reactivePulsarListener.consumerCustomizer(),
					"consumerCustomizer");
			if (StringUtils.hasText(consumerCustomizerBeanName)) {
				endpoint.setConsumerCustomizer(this.beanFactory.getBean(consumerCustomizerBeanName,
						ReactiveMessageConsumerBuilderCustomizer.class));
			}
		}
	}

	private Integer resolveExpressionAsInteger(String value, String attribute) {
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

	private Boolean resolveExpressionAsBoolean(String value, String attribute) {
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

	private void loadProperty(Properties properties, String property, Object value) {
		try {
			properties.load(new StringReader((String) value));
		}
		catch (IOException e) {
			this.logger.error(e, () -> "Failed to load property " + property + ", continuing...");
		}
	}

	private String getEndpointSubscriptionName(ReactivePulsarListener reactivePulsarListener) {
		if (StringUtils.hasText(reactivePulsarListener.subscriptionName())) {
			return resolveExpressionAsString(reactivePulsarListener.subscriptionName(), "subscriptionName");
		}
		return GENERATED_ID_PREFIX + this.counter.getAndIncrement();
	}

	private String getEndpointId(ReactivePulsarListener reactivePulsarListener) {
		if (StringUtils.hasText(reactivePulsarListener.id())) {
			return resolveExpressionAsString(reactivePulsarListener.id(), "id");
		}
		return GENERATED_ID_PREFIX + this.counter.getAndIncrement();
	}

	private String getTopicPattern(ReactivePulsarListener reactivePulsarListener) {
		return resolveExpressionAsString(reactivePulsarListener.topicPattern(), "topicPattern");
	}

	private String resolveExpressionAsString(String value, String attribute) {
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

	private String[] resolveTopics(ReactivePulsarListener ReactivePulsarListener) {
		String[] topics = ReactivePulsarListener.topics();
		List<String> result = new ArrayList<>();
		if (topics.length > 0) {
			for (String topic1 : topics) {
				Object topic = resolveExpression(topic1);
				resolveAsString(topic, result);
			}
		}
		return result.toArray(new String[0]);
	}

	private Object resolveExpression(String value) {
		return this.resolver.evaluate(resolve(value), this.expressionContext);
	}

	private String resolve(String value) {
		if (this.beanFactory != null && this.beanFactory instanceof ConfigurableBeanFactory) {
			return ((ConfigurableBeanFactory) this.beanFactory).resolveEmbeddedValue(value);
		}
		return value;
	}

	@SuppressWarnings("unchecked")
	private void resolveAsString(Object resolvedValue, List<String> result) {
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
					"@ReactivePulsarListener can't resolve '%s' as a String".formatted(resolvedValue));
		}
	}

	private Method checkProxy(Method methodArg, Object bean) {
		Method method = methodArg;
		if (AopUtils.isJdkDynamicProxy(bean)) {
			try {
				// Found a @ReactivePulsarListener method on the target class for this JDK
				// proxy
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
				throw new IllegalStateException("@ReactivePulsarListener method '%s' found on bean target class '%s', "
						+ "but not found in any interface(s) for bean JDK proxy. Either "
						+ "pull the method up to an interface or switch to subclass (CGLIB) "
						+ "proxies by setting proxy-target-class/proxyTargetClass "
						+ "attribute to 'true'".formatted(method.getName(), method.getDeclaringClass().getSimpleName()),
						ex);
			}
		}
		return method;
	}

	private Collection<ReactivePulsarListener> findListenerAnnotations(Class<?> clazz) {
		Set<ReactivePulsarListener> listeners = new HashSet<>();
		ReactivePulsarListener ann = AnnotatedElementUtils.findMergedAnnotation(clazz, ReactivePulsarListener.class);
		if (ann != null) {
			ann = enhance(clazz, ann);
			listeners.add(ann);
		}
		ReactivePulsarListeners anns = AnnotationUtils.findAnnotation(clazz, ReactivePulsarListeners.class);
		if (anns != null) {
			listeners.addAll(Arrays.stream(anns.value()).map(anno -> enhance(clazz, anno)).toList());
		}
		return listeners;
	}

	private Set<ReactivePulsarListener> findListenerAnnotations(Method method) {
		Set<ReactivePulsarListener> listeners = new HashSet<>();
		ReactivePulsarListener ann = AnnotatedElementUtils.findMergedAnnotation(method, ReactivePulsarListener.class);
		if (ann != null) {
			ann = enhance(method, ann);
			listeners.add(ann);
		}
		ReactivePulsarListeners anns = AnnotationUtils.findAnnotation(method, ReactivePulsarListeners.class);
		if (anns != null) {
			listeners.addAll(Arrays.stream(anns.value()).map(anno -> enhance(method, anno)).toList());
		}
		return listeners;
	}

	private ReactivePulsarListener enhance(AnnotatedElement element, ReactivePulsarListener ann) {
		if (this.enhancer == null) {
			return ann;
		}
		return AnnotationUtils.synthesizeAnnotation(
				this.enhancer.apply(AnnotationUtils.getAnnotationAttributes(ann), element),
				ReactivePulsarListener.class, null);
	}

	private void addFormatters(FormatterRegistry registry) {
		this.beanFactory.getBeanProvider(Converter.class).forEach(registry::addConverter);
		this.beanFactory.getBeanProvider(GenericConverter.class).forEach(registry::addConverter);
		this.beanFactory.getBeanProvider(Formatter.class).forEach(registry::addFormatter);
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

	public void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
		if (beanFactory instanceof ConfigurableListableBeanFactory) {
			this.resolver = ((ConfigurableListableBeanFactory) beanFactory).getBeanExpressionResolver();
			this.expressionContext = new BeanExpressionContext((ConfigurableListableBeanFactory) beanFactory,
					this.listenerScope);
		}
	}

	private class PulsarHandlerMethodFactoryAdapter implements MessageHandlerMethodFactory {

		private final DefaultFormattingConversionService defaultFormattingConversionService = new DefaultFormattingConversionService();

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
			defaultFactory.setBeanFactory(ReactivePulsarListenerAnnotationBeanPostProcessor.this.beanFactory);
			this.defaultFormattingConversionService.addConverter(
					new BytesToStringConverter(ReactivePulsarListenerAnnotationBeanPostProcessor.this.charset));
			this.defaultFormattingConversionService.addConverter(new BytesToNumberConverter());
			defaultFactory.setConversionService(this.defaultFormattingConversionService);
			GenericMessageConverter messageConverter = new GenericMessageConverter(
					this.defaultFormattingConversionService);
			defaultFactory.setMessageConverter(messageConverter);

			defaultFactory.afterPropertiesSet();

			return defaultFactory;
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

	static class ListenerScope implements Scope {

		private final Map<String, Object> listeners = new HashMap<>();

		ListenerScope() {
		}

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

	public interface AnnotationEnhancer extends BiFunction<Map<String, Object>, AnnotatedElement, Map<String, Object>> {

	}

}
