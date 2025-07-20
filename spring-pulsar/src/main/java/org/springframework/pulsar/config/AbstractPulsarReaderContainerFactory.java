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

package org.springframework.pulsar.config;

import org.apache.pulsar.client.api.Schema;
import org.jspecify.annotations.Nullable;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.core.PulsarReaderFactory;
import org.springframework.pulsar.reader.AbstractPulsarMessageReaderContainer;
import org.springframework.pulsar.reader.PulsarMessageReaderContainer;
import org.springframework.pulsar.reader.PulsarReaderContainerProperties;
import org.springframework.pulsar.support.JavaUtils;
import org.springframework.pulsar.support.MessageConverter;

/**
 * Base {@link PulsarReaderContainerFactory} implementation.
 *
 * @param <C> the {@link AbstractPulsarMessageReaderContainer} implementation type.
 * @param <T> Message payload type.
 * @author Soby Chacko
 */
public abstract class AbstractPulsarReaderContainerFactory<C extends AbstractPulsarMessageReaderContainer<T>, T>
		implements PulsarReaderContainerFactory, ApplicationEventPublisherAware, ApplicationContextAware {

	protected final LogAccessor logger = new LogAccessor(this.getClass());

	private final PulsarReaderFactory<? super T> readerFactory;

	private final PulsarReaderContainerProperties containerProperties;

	private @Nullable Boolean autoStartup;

	private @Nullable Integer phase;

	private @Nullable MessageConverter messageConverter;

	private @Nullable ApplicationEventPublisher applicationEventPublisher;

	private @Nullable ApplicationContext applicationContext;

	protected AbstractPulsarReaderContainerFactory(PulsarReaderFactory<? super T> readerFactory,
			PulsarReaderContainerProperties containerProperties) {
		this.readerFactory = readerFactory;
		this.containerProperties = containerProperties;
	}

	protected PulsarReaderFactory<? super T> getReaderFactory() {
		return this.readerFactory;
	}

	public PulsarReaderContainerProperties getContainerProperties() {
		return this.containerProperties;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public void setAutoStartup(Boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	public void setPhase(int phase) {
		this.phase = phase;
	}

	/**
	 * Set the message converter to use if dynamic argument type matching is needed.
	 * @param messageConverter the converter.
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	@SuppressWarnings("unchecked")
	@Override
	public C createRegisteredContainer(PulsarReaderEndpoint<PulsarMessageReaderContainer> endpoint) {
		C instance = createContainerInstance(endpoint);
		JavaUtils.INSTANCE.acceptIfNotNull(endpoint.getId(), instance::setBeanName);
		if (endpoint instanceof AbstractPulsarReaderEndpoint) {
			configureEndpoint((AbstractPulsarReaderEndpoint<C>) endpoint);
		}

		endpoint.setupListenerContainer(instance, this.messageConverter);
		initializeContainer(instance, endpoint);
		// customizeContainer(instance);
		return instance;
	}

	protected abstract C createContainerInstance(PulsarReaderEndpoint<PulsarMessageReaderContainer> endpoint);

	private void configureEndpoint(AbstractPulsarReaderEndpoint<C> aplEndpoint) {

	}

	@SuppressWarnings("NullAway")
	protected void initializeContainer(C instance, PulsarReaderEndpoint<PulsarMessageReaderContainer> endpoint) {
		PulsarReaderContainerProperties instanceProperties = instance.getContainerProperties();

		if (instanceProperties.getSchema() == null) {
			JavaUtils.INSTANCE.acceptIfNotNull(this.containerProperties.getSchema(), instanceProperties::setSchema);
		}

		if (instanceProperties.getSchema() == null) {
			instanceProperties.setSchema(Schema.BYTES);
		}

		Boolean autoStart = endpoint.getAutoStartup();
		if (autoStart != null) {
			instance.setAutoStartup(autoStart);
		}
		else if (this.autoStartup != null) {
			instance.setAutoStartup(this.autoStartup);
		}

		JavaUtils.INSTANCE.acceptIfNotNull(this.phase, instance::setPhase)
			.acceptIfNotNull(this.applicationContext, instance::setApplicationContext)
			.acceptIfNotNull(this.applicationEventPublisher, instance::setApplicationEventPublisher);
	}

}
