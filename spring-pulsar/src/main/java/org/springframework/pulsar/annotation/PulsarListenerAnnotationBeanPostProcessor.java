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

package org.springframework.pulsar.annotation;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.RedeliveryBackoff;

import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.format.Formatter;
import org.springframework.format.FormatterRegistry;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.config.MethodPulsarListenerEndpoint;
import org.springframework.pulsar.config.PulsarAnnotationSupportBeanNames;
import org.springframework.pulsar.config.PulsarListenerContainerFactory;
import org.springframework.pulsar.config.PulsarListenerEndpoint;
import org.springframework.pulsar.config.PulsarListenerEndpointRegistrar;
import org.springframework.pulsar.config.PulsarListenerEndpointRegistry;
import org.springframework.pulsar.listener.PulsarConsumerErrorHandler;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Bean post-processor that registers methods annotated with {@link PulsarListener} to be
 * invoked by a Pulsar message listener container created under the covers by a
 * {@link PulsarListenerContainerFactory} according to the parameters of the annotation.
 *
 * <p>
 * Annotated methods can use flexible arguments as defined by {@link PulsarListener}.
 *
 * <p>
 * This post-processor is automatically registered by the {@link EnablePulsar} annotation.
 *
 * <p>
 * Auto-detect any {@link PulsarListenerConfigurer} instances in the container, allowing
 * for customization of the registry to be used, the default container factory or for
 * fine-grained control over endpoints registration. See {@link EnablePulsar} Javadoc for
 * complete usage details.
 *
 * @param <V> the payload type.
 * @author Soby Chacko
 * @author Chris Bono
 * @author Alexander Preuß
 * @see PulsarListener
 * @see EnablePulsar
 * @see PulsarListenerConfigurer
 * @see PulsarListenerEndpointRegistrar
 * @see PulsarListenerEndpointRegistry
 * @see PulsarListenerEndpoint
 * @see MethodPulsarListenerEndpoint
 */
public class PulsarListenerAnnotationBeanPostProcessor<V> extends AbstractPulsarAnnotationsBeanPostProcessor
		implements SmartInitializingSingleton {

	/**
	 * The bean name of the default
	 * {@link org.springframework.pulsar.config.PulsarListenerContainerFactory}.
	 */
	public static final String DEFAULT_PULSAR_LISTENER_CONTAINER_FACTORY_BEAN_NAME = "pulsarListenerContainerFactory";

	private static final String GENERATED_ID_PREFIX = "org.springframework.Pulsar.PulsarListenerEndpointContainer#";

	private PulsarListenerEndpointRegistry endpointRegistry;

	private String defaultContainerFactoryBeanName = DEFAULT_PULSAR_LISTENER_CONTAINER_FACTORY_BEAN_NAME;

	private final PulsarListenerEndpointRegistrar registrar = new PulsarListenerEndpointRegistrar(
			PulsarListenerContainerFactory.class);

	private final Set<Class<?>> nonAnnotatedClasses = Collections.newSetFromMap(new ConcurrentHashMap<>(64));

	private final ListenerScope listenerScope = new ListenerScope();

	private AnnotationEnhancer enhancer;

	private final AtomicInteger counter = new AtomicInteger();

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
						PulsarAnnotationSupportBeanNames.PULSAR_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME,
						PulsarListenerEndpointRegistry.class);
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
	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
		if (!this.nonAnnotatedClasses.contains(bean.getClass())) {
			Class<?> targetClass = AopUtils.getTargetClass(bean);
			Map<Method, Set<PulsarListener>> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
					(MethodIntrospector.MetadataLookup<Set<PulsarListener>>) method -> {
						Set<PulsarListener> listenerMethods = findListenerAnnotations(method);
						return (!listenerMethods.isEmpty() ? listenerMethods : null);
					});
			if (annotatedMethods.isEmpty()) {
				this.nonAnnotatedClasses.add(bean.getClass());
				this.logger.trace(() -> "No @PulsarListener annotations found on bean type: " + bean.getClass());
			}
			else {
				// Non-empty set of methods
				for (Map.Entry<Method, Set<PulsarListener>> entry : annotatedMethods.entrySet()) {
					Method method = entry.getKey();
					for (PulsarListener listener : entry.getValue()) {
						processPulsarListener(listener, method, bean, beanName);
					}
				}
				this.logger.debug(() -> annotatedMethods.size() + " @PulsarListener methods processed on bean '"
						+ beanName + "': " + annotatedMethods);
			}
		}
		return bean;
	}

	protected void processPulsarListener(PulsarListener pulsarListener, Method method, Object bean, String beanName) {
		Method methodToUse = checkProxy(method, bean);
		MethodPulsarListenerEndpoint<V> endpoint = new MethodPulsarListenerEndpoint<>();
		endpoint.setMethod(methodToUse);

		String beanRef = pulsarListener.beanRef();
		this.listenerScope.addListener(beanRef, bean);
		String[] topics = resolveTopics(pulsarListener);
		String topicPattern = getTopicPattern(pulsarListener);
		processListener(endpoint, pulsarListener, bean, beanName, topics, topicPattern);
		this.listenerScope.removeListener(beanRef);
	}

	protected void processListener(MethodPulsarListenerEndpoint<?> endpoint, PulsarListener PulsarListener, Object bean,
			String beanName, String[] topics, String topicPattern) {

		processPulsarListenerAnnotation(endpoint, PulsarListener, bean, topics, topicPattern);

		String containerFactory = resolve(PulsarListener.containerFactory());
		PulsarListenerContainerFactory listenerContainerFactory = resolveContainerFactory(PulsarListener,
				containerFactory, beanName);

		this.registrar.registerEndpoint(endpoint, listenerContainerFactory);
	}

	@Nullable
	private PulsarListenerContainerFactory resolveContainerFactory(PulsarListener PulsarListener, Object factoryTarget,
			String beanName) {

		String containerFactory = PulsarListener.containerFactory();
		if (!StringUtils.hasText(containerFactory)) {
			return null;
		}

		PulsarListenerContainerFactory factory = null;

		Object resolved = resolveExpression(containerFactory);
		if (resolved instanceof PulsarListenerContainerFactory) {
			return (PulsarListenerContainerFactory) resolved;
		}
		String containerFactoryBeanName = resolveExpressionAsString(containerFactory, "containerFactory");
		if (StringUtils.hasText(containerFactoryBeanName)) {
			assertBeanFactory();
			try {
				factory = this.beanFactory.getBean(containerFactoryBeanName, PulsarListenerContainerFactory.class);
			}
			catch (NoSuchBeanDefinitionException ex) {
				throw new BeanInitializationException(noBeanFoundMessage(factoryTarget, beanName,
						containerFactoryBeanName, PulsarListenerContainerFactory.class), ex);
			}
		}
		return factory;
	}

	private void processPulsarListenerAnnotation(MethodPulsarListenerEndpoint<?> endpoint,
			PulsarListener pulsarListener, Object bean, String[] topics, String topicPattern) {

		endpoint.setBean(bean);
		endpoint.setMessageHandlerMethodFactory(this.messageHandlerMethodFactory);
		endpoint.setSubscriptionName(getEndpointSubscriptionName(pulsarListener));
		endpoint.setId(getEndpointId(pulsarListener));
		endpoint.setTopics(topics);
		endpoint.setTopicPattern(topicPattern);
		endpoint.setSubscriptionType(pulsarListener.subscriptionType());
		endpoint.setSchemaType(pulsarListener.schemaType());
		endpoint.setAckMode(pulsarListener.ackMode());

		String concurrency = pulsarListener.concurrency();
		if (StringUtils.hasText(concurrency)) {
			endpoint.setConcurrency(resolveExpressionAsInteger(concurrency, "concurrency"));
		}

		String autoStartup = pulsarListener.autoStartup();
		if (StringUtils.hasText(autoStartup)) {
			endpoint.setAutoStartup(resolveExpressionAsBoolean(autoStartup, "autoStartup"));
		}
		resolvePulsarProperties(endpoint, pulsarListener.properties());
		endpoint.setBatchListener(pulsarListener.batch());
		endpoint.setBeanFactory(this.beanFactory);

		resolveNegativeAckRedeliveryBackoff(endpoint, pulsarListener);
		resolveAckTimeoutRedeliveryBackoff(endpoint, pulsarListener);
		resolveDeadLetterPolicy(endpoint, pulsarListener);
		resolvePulsarConsumerErrorHandler(endpoint, pulsarListener);
	}

	@SuppressWarnings({ "rawtypes" })
	private void resolvePulsarConsumerErrorHandler(MethodPulsarListenerEndpoint<?> endpoint,
			PulsarListener pulsarListener) {
		Object pulsarConsumerErrorHandler = resolveExpression(pulsarListener.pulsarConsumerErrorHandler());
		if (pulsarConsumerErrorHandler instanceof PulsarConsumerErrorHandler) {
			endpoint.setPulsarConsumerErrorHandler((PulsarConsumerErrorHandler) pulsarConsumerErrorHandler);
		}
		else {
			String pulsarConsumerErrorHandlerBeanName = resolveExpressionAsString(
					pulsarListener.pulsarConsumerErrorHandler(), "pulsarConsumerErrorHandler");
			if (StringUtils.hasText(pulsarConsumerErrorHandlerBeanName)) {
				endpoint.setPulsarConsumerErrorHandler(
						this.beanFactory.getBean(pulsarConsumerErrorHandlerBeanName, PulsarConsumerErrorHandler.class));
			}
		}
	}

	private void resolveNegativeAckRedeliveryBackoff(MethodPulsarListenerEndpoint<?> endpoint,
			PulsarListener pulsarListener) {
		Object negativeAckRedeliveryBackoff = resolveExpression(pulsarListener.negativeAckRedeliveryBackoff());
		if (negativeAckRedeliveryBackoff instanceof RedeliveryBackoff) {
			endpoint.setNegativeAckRedeliveryBackoff((RedeliveryBackoff) negativeAckRedeliveryBackoff);
		}
		else {
			String negativeAckRedeliveryBackoffBeanName = resolveExpressionAsString(
					pulsarListener.negativeAckRedeliveryBackoff(), "negativeAckRedeliveryBackoff");
			if (StringUtils.hasText(negativeAckRedeliveryBackoffBeanName)) {
				endpoint.setNegativeAckRedeliveryBackoff(
						this.beanFactory.getBean(negativeAckRedeliveryBackoffBeanName, RedeliveryBackoff.class));
			}
		}
	}

	private void resolveAckTimeoutRedeliveryBackoff(MethodPulsarListenerEndpoint<?> endpoint,
			PulsarListener pulsarListener) {
		Object ackTimeoutRedeliveryBackoff = resolveExpression(pulsarListener.ackTimeoutRedeliveryBackoff());
		if (ackTimeoutRedeliveryBackoff instanceof RedeliveryBackoff) {
			endpoint.setAckTimeoutRedeliveryBackoff((RedeliveryBackoff) ackTimeoutRedeliveryBackoff);
		}
		else {
			String ackTimeoutRedeliveryBackoffBeanName = resolveExpressionAsString(
					pulsarListener.ackTimeoutRedeliveryBackoff(), "ackTimeoutRedeliveryBackoff");
			if (StringUtils.hasText(ackTimeoutRedeliveryBackoffBeanName)) {
				endpoint.setAckTimeoutRedeliveryBackoff(
						this.beanFactory.getBean(ackTimeoutRedeliveryBackoffBeanName, RedeliveryBackoff.class));
			}
		}
	}

	private void resolveDeadLetterPolicy(MethodPulsarListenerEndpoint<?> endpoint, PulsarListener pulsarListener) {
		Object deadLetterPolicy = resolveExpression(pulsarListener.deadLetterPolicy());
		if (deadLetterPolicy instanceof DeadLetterPolicy) {
			endpoint.setDeadLetterPolicy((DeadLetterPolicy) deadLetterPolicy);
		}
		else {
			String deadLetterPolicyBeanName = resolveExpressionAsString(pulsarListener.deadLetterPolicy(),
					"deadLetterPolicy");
			if (StringUtils.hasText(deadLetterPolicyBeanName)) {
				endpoint.setDeadLetterPolicy(
						this.beanFactory.getBean(deadLetterPolicyBeanName, DeadLetterPolicy.class));
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void resolvePulsarProperties(MethodPulsarListenerEndpoint<?> endpoint, String[] propertyStrings) {
		if (propertyStrings.length > 0) {
			Properties properties = new Properties();
			for (String property : propertyStrings) {
				Object value = resolveExpression(property);
				if (value instanceof String) {
					loadProperty(properties, property, value);
				}
				else if (value instanceof String[]) {
					for (String prop : (String[]) value) {
						loadProperty(properties, prop, prop);
					}
				}
				else if (value instanceof Collection<?> values) {
					if (values.size() > 0 && values.iterator().next() instanceof String) {
						for (String prop : (Collection<String>) value) {
							loadProperty(properties, prop, prop);
						}
					}
				}
				else {
					throw new IllegalStateException(
							"'properties' must resolve to a String, a String[] or " + "Collection<String>");
				}
			}
			endpoint.setConsumerProperties(properties);
		}
	}

	private String getEndpointSubscriptionName(PulsarListener pulsarListener) {
		if (StringUtils.hasText(pulsarListener.subscriptionName())) {
			return resolveExpressionAsString(pulsarListener.subscriptionName(), "subscriptionName");
		}
		return GENERATED_ID_PREFIX + this.counter.getAndIncrement();
	}

	private String getEndpointId(PulsarListener pulsarListener) {
		if (StringUtils.hasText(pulsarListener.id())) {
			return resolveExpressionAsString(pulsarListener.id(), "id");
		}
		return GENERATED_ID_PREFIX + this.counter.getAndIncrement();
	}

	private String getTopicPattern(PulsarListener pulsarListener) {
		return resolveExpressionAsString(pulsarListener.topicPattern(), "topicPattern");
	}

	private String[] resolveTopics(PulsarListener PulsarListener) {
		String[] topics = PulsarListener.topics();
		List<String> result = new ArrayList<>();
		if (topics.length > 0) {
			for (String topic1 : topics) {
				Object topic = resolveExpression(topic1);
				resolveAsString(topic, result);
			}
		}
		return result.toArray(new String[0]);
	}

	private Set<PulsarListener> findListenerAnnotations(Method method) {
		Set<PulsarListener> listeners = new HashSet<>();
		PulsarListener ann = AnnotatedElementUtils.findMergedAnnotation(method, PulsarListener.class);
		if (ann != null) {
			ann = enhance(method, ann);
			listeners.add(ann);
		}
		PulsarListeners anns = AnnotationUtils.findAnnotation(method, PulsarListeners.class);
		if (anns != null) {
			listeners.addAll(Arrays.stream(anns.value()).map(anno -> enhance(method, anno)).toList());
		}
		return listeners;
	}

	private PulsarListener enhance(AnnotatedElement element, PulsarListener ann) {
		if (this.enhancer == null) {
			return ann;
		}
		return AnnotationUtils.synthesizeAnnotation(
				this.enhancer.apply(AnnotationUtils.getAnnotationAttributes(ann), element), PulsarListener.class, null);
	}

	private void addFormatters(FormatterRegistry registry) {
		this.beanFactory.getBeanProvider(Converter.class).forEach(registry::addConverter);
		this.beanFactory.getBeanProvider(GenericConverter.class).forEach(registry::addConverter);
		this.beanFactory.getBeanProvider(Formatter.class).forEach(registry::addFormatter);
	}

}
