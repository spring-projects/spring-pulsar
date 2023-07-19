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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.pulsar.client.api.MessageId;

import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.config.MethodPulsarReaderEndpoint;
import org.springframework.pulsar.config.PulsarAnnotationSupportBeanNames;
import org.springframework.pulsar.config.PulsarReaderContainerFactory;
import org.springframework.pulsar.config.PulsarReaderEndpoint;
import org.springframework.pulsar.config.PulsarReaderEndpointRegistrar;
import org.springframework.pulsar.config.PulsarReaderEndpointRegistry;
import org.springframework.pulsar.core.ConsumerBuilderCustomizer;
import org.springframework.pulsar.core.ReaderBuilderCustomizer;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Bean post-processor that registers methods annotated with {@link PulsarListener} to be
 * invoked by a Pulsar message listener container created under the covers by a
 * {@link PulsarReaderContainerFactory} according to the parameters of the annotation.
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
 * @see PulsarReader
 * @see EnablePulsar
 * @see PulsarReaderConfigurer
 * @see PulsarReaderEndpointRegistrar
 * @see PulsarReaderEndpointRegistry
 * @see PulsarReaderEndpoint
 * @see MethodPulsarReaderEndpoint
 */
public class PulsarReaderAnnotationBeanPostProcessor<V> extends AbstractPulsarAnnotationsBeanPostProcessor
		implements SmartInitializingSingleton {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	/**
	 * The bean name of the default
	 * {@link org.springframework.pulsar.config.PulsarReaderContainerFactory}.
	 */
	public static final String DEFAULT_PULSAR_READER_CONTAINER_FACTORY_BEAN_NAME = "pulsarReaderContainerFactory";

	private static final String GENERATED_ID_PREFIX = "org.springframework.Pulsar.PulsarReaderEndpointContainer#";

	private ApplicationContext applicationContext;

	private PulsarReaderEndpointRegistry endpointRegistry;

	private String defaultContainerFactoryBeanName = DEFAULT_PULSAR_READER_CONTAINER_FACTORY_BEAN_NAME;

	private final PulsarReaderEndpointRegistrar registrar = new PulsarReaderEndpointRegistrar(
			PulsarReaderContainerFactory.class);

	private final Set<Class<?>> nonAnnotatedClasses = Collections.newSetFromMap(new ConcurrentHashMap<>(64));

	private final AtomicInteger counter = new AtomicInteger();

	@Override
	public void afterSingletonsInstantiated() {
		this.registrar.setBeanFactory(this.beanFactory);

		this.beanFactory.getBeanProvider(PulsarReaderConfigurer.class)
			.forEach(c -> c.configurePulsarReaders(this.registrar));

		if (this.registrar.getEndpointRegistry() == null) {
			if (this.endpointRegistry == null) {
				Assert.state(this.beanFactory != null,
						"BeanFactory must be set to find endpoint registry by bean name");
				this.endpointRegistry = this.beanFactory.getBean(
						PulsarAnnotationSupportBeanNames.PULSAR_READER_ENDPOINT_REGISTRY_BEAN_NAME,
						PulsarReaderEndpointRegistry.class);
			}
			this.registrar.setEndpointRegistry(this.endpointRegistry);
		}

		if (this.defaultContainerFactoryBeanName != null) {
			this.registrar.setContainerFactoryBeanName(this.defaultContainerFactoryBeanName);
		}

		// Register all readers
		this.registrar.afterPropertiesSet();
	}

	@Override
	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
		if (!this.nonAnnotatedClasses.contains(bean.getClass())) {
			Class<?> targetClass = AopUtils.getTargetClass(bean);
			Map<Method, Set<PulsarReader>> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
					(MethodIntrospector.MetadataLookup<Set<PulsarReader>>) method -> {
						Set<PulsarReader> readerMethods = findReaderAnnotations(method);
						return (!readerMethods.isEmpty() ? readerMethods : null);
					});
			if (annotatedMethods.isEmpty()) {
				this.nonAnnotatedClasses.add(bean.getClass());
				this.logger.trace(() -> "No @PulsarReader annotations found on bean type: " + bean.getClass());
			}
			else {
				// Non-empty set of methods
				for (Map.Entry<Method, Set<PulsarReader>> entry : annotatedMethods.entrySet()) {
					Method method = entry.getKey();
					for (PulsarReader listener : entry.getValue()) {
						processPulsarReader(listener, method, bean, beanName);
					}
				}
				this.logger.debug(() -> annotatedMethods.size() + " @PulsarListener methods processed on bean '"
						+ beanName + "': " + annotatedMethods);
			}
		}
		return bean;
	}

	protected void processPulsarReader(PulsarReader pulsarReader, Method method, Object bean, String beanName) {
		Method methodToUse = checkProxy(method, bean);
		MethodPulsarReaderEndpoint<V> endpoint = new MethodPulsarReaderEndpoint<>();
		endpoint.setMethod(methodToUse);

		String beanRef = pulsarReader.beanRef();
		this.listenerScope.addListener(beanRef, bean);
		String[] topics = resolveTopics(pulsarReader);
		processReader(endpoint, pulsarReader, bean, beanName, topics);
		this.listenerScope.removeListener(beanRef);
	}

	protected void processReader(MethodPulsarReaderEndpoint<?> endpoint, PulsarReader pulsarReader, Object bean,
			String beanName, String[] topics) {

		processPulsarReaderAnnotation(endpoint, pulsarReader, bean, topics);

		String containerFactory = resolve(pulsarReader.containerFactory());
		PulsarReaderContainerFactory listenerContainerFactory = resolveContainerFactory(pulsarReader, containerFactory,
				beanName);

		this.registrar.registerEndpoint(endpoint, listenerContainerFactory);
	}

	@Nullable
	private PulsarReaderContainerFactory resolveContainerFactory(PulsarReader pulsarReader, Object factoryTarget,
			String beanName) {
		String containerFactory = pulsarReader.containerFactory();
		if (!StringUtils.hasText(containerFactory)) {
			return null;
		}
		PulsarReaderContainerFactory factory = null;
		Object resolved = resolveExpression(containerFactory);
		if (resolved instanceof PulsarReaderContainerFactory) {
			return (PulsarReaderContainerFactory) resolved;
		}
		String containerFactoryBeanName = resolveExpressionAsString(containerFactory, "containerFactory");
		if (StringUtils.hasText(containerFactoryBeanName)) {
			assertBeanFactory();
			try {
				factory = this.beanFactory.getBean(containerFactoryBeanName, PulsarReaderContainerFactory.class);
			}
			catch (NoSuchBeanDefinitionException ex) {
				throw new BeanInitializationException(noBeanFoundMessage(factoryTarget, beanName,
						containerFactoryBeanName, PulsarReaderContainerFactory.class), ex);
			}
		}
		return factory;
	}

	private void processPulsarReaderAnnotation(MethodPulsarReaderEndpoint<?> endpoint, PulsarReader pulsarReader,
			Object bean, String[] topics) {
		endpoint.setBean(bean);
		endpoint.setMessageHandlerMethodFactory(this.messageHandlerMethodFactory);
		endpoint.setSubscriptionName(getEndpointSubscriptionName(pulsarReader));
		endpoint.setId(getEndpointId(pulsarReader));
		endpoint.setTopics(topics);
		endpoint.setSchemaType(pulsarReader.schemaType());
		String startMessageIdString = pulsarReader.startMessageId();
		MessageId startMessageId = null;
		if (startMessageIdString.equalsIgnoreCase("earliest")) {
			startMessageId = MessageId.earliest;
		}
		else if (startMessageIdString.equalsIgnoreCase("latest")) {
			startMessageId = MessageId.latest;
		}
		endpoint.setStartMessageId(startMessageId);

		String autoStartup = pulsarReader.autoStartup();
		if (StringUtils.hasText(autoStartup)) {
			endpoint.setAutoStartup(resolveExpressionAsBoolean(autoStartup, "autoStartup"));
		}
		endpoint.setBeanFactory(this.beanFactory);

		resolveReaderCustomizer(endpoint, pulsarReader);
	}

	private void resolveReaderCustomizer(MethodPulsarReaderEndpoint<?> endpoint, PulsarReader pulsarReader) {
		Object readerCustomizer = resolveExpression(pulsarReader.readerCustomizer());
		if (readerCustomizer instanceof ConsumerBuilderCustomizer<?>) {
			endpoint.setReaderBuilderCustomizer((ReaderBuilderCustomizer<?>) readerCustomizer);
		}
		else {
			String readerCustomizerBeanName = resolveExpressionAsString(pulsarReader.readerCustomizer(),
					"readerCustomizer");
			if (StringUtils.hasText(readerCustomizerBeanName)) {
				endpoint.setReaderBuilderCustomizer(
						this.beanFactory.getBean(readerCustomizerBeanName, ReaderBuilderCustomizer.class));
			}
		}
	}

	private String getEndpointSubscriptionName(PulsarReader pulsarReader) {
		if (StringUtils.hasText(pulsarReader.subscriptionName())) {
			return resolveExpressionAsString(pulsarReader.subscriptionName(), "subscriptionName");
		}
		return GENERATED_ID_PREFIX + this.counter.getAndIncrement();
	}

	private String getEndpointId(PulsarReader pulsarReader) {
		if (StringUtils.hasText(pulsarReader.id())) {
			return resolveExpressionAsString(pulsarReader.id(), "id");
		}
		return GENERATED_ID_PREFIX + this.counter.getAndIncrement();
	}

	private String[] resolveTopics(PulsarReader PulsarListener) {
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

	private Set<PulsarReader> findReaderAnnotations(Method method) {
		Set<PulsarReader> readers = new HashSet<>();
		PulsarReader ann = AnnotatedElementUtils.findMergedAnnotation(method, PulsarReader.class);
		if (ann != null) {
			readers.add(ann);
		}
		return readers;
	}

}
