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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.pulsar.client.api.DeadLetterPolicy;

import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.format.Formatter;
import org.springframework.format.FormatterRegistry;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.annotation.AbstractPulsarAnnotationsBeanPostProcessor;
import org.springframework.pulsar.annotation.PulsarHeaderObjectMapperUtils;
import org.springframework.pulsar.annotation.PulsarListenerConfigurer;
import org.springframework.pulsar.config.PulsarAnnotationSupportBeanNames;
import org.springframework.pulsar.config.PulsarListenerEndpointRegistrar;
import org.springframework.pulsar.reactive.config.MethodReactivePulsarListenerEndpoint;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerContainerFactory;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerEndpoint;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerEndpointRegistry;
import org.springframework.util.Assert;
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
 * @author Soby Chacko
 * @author Jihoon Kim
 * @see ReactivePulsarListener
 * @see EnableReactivePulsar
 * @see PulsarListenerConfigurer
 * @see PulsarListenerEndpointRegistrar
 * @see ReactivePulsarListenerEndpointRegistry
 * @see ReactivePulsarListenerEndpoint
 * @see MethodReactivePulsarListenerEndpoint
 */
public class ReactivePulsarListenerAnnotationBeanPostProcessor<V> extends AbstractPulsarAnnotationsBeanPostProcessor
		implements SmartInitializingSingleton {

	/**
	 * The bean name of the default {@link ReactivePulsarListenerContainerFactory}.
	 */
	public static final String DEFAULT_REACTIVE_PULSAR_LISTENER_CONTAINER_FACTORY_BEAN_NAME = "reactivePulsarListenerContainerFactory";

	private static final String GENERATED_ID_PREFIX = "org.springframework.Pulsar.ReactivePulsarListenerEndpointContainer#";

	private ReactivePulsarListenerEndpointRegistry<?> endpointRegistry;

	private String defaultContainerFactoryBeanName = DEFAULT_REACTIVE_PULSAR_LISTENER_CONTAINER_FACTORY_BEAN_NAME;

	private final PulsarListenerEndpointRegistrar registrar = new PulsarListenerEndpointRegistrar(
			ReactivePulsarListenerContainerFactory.class);

	private final Set<Class<?>> nonAnnotatedClasses = Collections.newSetFromMap(new ConcurrentHashMap<>(64));

	private final ListenerScope listenerScope = new ListenerScope();

	private final AtomicInteger counter = new AtomicInteger();

	private final List<MethodReactivePulsarListenerEndpoint<?>> processedEndpoints = new ArrayList<>();

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
		addFormatters(this.messageHandlerMethodFactory.getDefaultFormattingConversionService());
		postProcessEndpointsBeforeRegistration();
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
				this.logger
					.trace(() -> "No @ReactivePulsarListener annotations found on bean type: " + bean.getClass());
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

	private void processReactivePulsarListenerAnnotation(MethodReactivePulsarListenerEndpoint<?> endpoint,
			ReactivePulsarListener reactivePulsarListener, Object bean, String[] topics, String topicPattern) {
		endpoint.setBean(bean);
		endpoint.setMessageHandlerMethodFactory(this.messageHandlerMethodFactory);
		endpoint.setSubscriptionName(getEndpointSubscriptionName(reactivePulsarListener));
		endpoint.setId(getEndpointId(reactivePulsarListener));
		endpoint.setTopics(topics);
		endpoint.setTopicPattern(topicPattern);
		resolveSubscriptionType(endpoint, reactivePulsarListener);
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
		this.processedEndpoints.add(endpoint);
	}

	private void resolveSubscriptionType(MethodReactivePulsarListenerEndpoint<?> endpoint,
			ReactivePulsarListener reactivePulsarListener) {
		Assert.state(reactivePulsarListener.subscriptionType().length <= 1,
				() -> "ReactivePulsarListener.subscriptionType must have 0 or 1 elements");
		if (reactivePulsarListener.subscriptionType().length == 1) {
			endpoint.setSubscriptionType(reactivePulsarListener.subscriptionType()[0]);
		}
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
				endpoint
					.setDeadLetterPolicy(this.beanFactory.getBean(deadLetterPolicyBeanName, DeadLetterPolicy.class));
			}
		}
	}

	@SuppressWarnings("unchecked")
	protected void postProcessEndpointsBeforeRegistration() {
		PulsarHeaderObjectMapperUtils.customMapper(this.beanFactory)
			.ifPresent((objectMapper) -> this.processedEndpoints
				.forEach((endpoint) -> endpoint.setObjectMapper(objectMapper)));
		if (this.processedEndpoints.size() == 1) {
			MethodReactivePulsarListenerEndpoint<?> endpoint = this.processedEndpoints.get(0);
			if (endpoint.getConsumerCustomizer() != null) {
				return;
			}
			this.beanFactory.getBeanProvider(ReactivePulsarListenerMessageConsumerBuilderCustomizer.class)
				.ifUnique((customizer) -> {
					this.logger.info(() -> String
						.format("Setting the only registered ReactivePulsarListenerMessageConsumerBuilderCustomizer "
								+ "on the only registered @ReactivePulsarListener (%s)", endpoint.getId()));
					endpoint.setConsumerCustomizer(customizer::customize);
				});
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void resolveConsumerCustomizer(MethodReactivePulsarListenerEndpoint<?> endpoint,
			ReactivePulsarListener reactivePulsarListener) {
		if (!StringUtils.hasText(reactivePulsarListener.consumerCustomizer())) {
			return;
		}
		Object consumerCustomizer = resolveExpression(reactivePulsarListener.consumerCustomizer());
		if (consumerCustomizer instanceof ReactivePulsarListenerMessageConsumerBuilderCustomizer customizer) {
			endpoint.setConsumerCustomizer(customizer::customize);
		}
		else {
			String customizerBeanName = resolveExpressionAsString(reactivePulsarListener.consumerCustomizer(),
					"consumerCustomizer");
			if (StringUtils.hasText(customizerBeanName)) {
				var customizer = this.beanFactory.getBean(customizerBeanName,
						ReactivePulsarListenerMessageConsumerBuilderCustomizer.class);
				endpoint.setConsumerCustomizer(customizer::customize);
			}
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

	private Collection<ReactivePulsarListener> findListenerAnnotations(Class<?> clazz) {
		Set<ReactivePulsarListener> listeners = new HashSet<>();
		ReactivePulsarListener ann = AnnotatedElementUtils.findMergedAnnotation(clazz, ReactivePulsarListener.class);
		if (ann != null) {
			listeners.add(ann);
		}
		ReactivePulsarListeners anns = AnnotationUtils.findAnnotation(clazz, ReactivePulsarListeners.class);
		if (anns != null) {
			listeners.addAll(Arrays.stream(anns.value()).toList());
		}
		return listeners;
	}

	private Set<ReactivePulsarListener> findListenerAnnotations(Method method) {
		Set<ReactivePulsarListener> listeners = new HashSet<>();
		ReactivePulsarListener ann = AnnotatedElementUtils.findMergedAnnotation(method, ReactivePulsarListener.class);
		if (ann != null) {
			listeners.add(ann);
		}
		ReactivePulsarListeners anns = AnnotationUtils.findAnnotation(method, ReactivePulsarListeners.class);
		if (anns != null) {
			listeners.addAll(Arrays.stream(anns.value()).toList());
		}
		return listeners;
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

}
