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

package org.springframework.pulsar.core;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.core.log.LogAccessor;
import org.springframework.util.Assert;

/**
 * Default implementation for {@link PulsarClientFactory}.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public class DefaultPulsarClientFactory implements PulsarClientFactory, EnvironmentAware {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private final PulsarClientBuilderCustomizer customizer;

	private boolean useRestartableClient;

	/**
	 * Construct a factory that creates clients using a default Pulsar client builder with
	 * no modifications other than the specified service url.
	 * @param serviceUrl the service url
	 */
	public DefaultPulsarClientFactory(String serviceUrl) {
		this((clientBuilder) -> clientBuilder.serviceUrl(serviceUrl));
	}

	/**
	 * Construct a factory that creates clients using a customized Pulsar client builder.
	 * @param customizer the customizer to apply to the builder
	 */
	public DefaultPulsarClientFactory(PulsarClientBuilderCustomizer customizer) {
		Assert.notNull(customizer, "customizer must not be null");
		this.customizer = customizer;
	}

	@Override
	public PulsarClient createClient() throws PulsarClientException {
		if (this.useRestartableClient) {
			this.logger.info(() -> "Using restartable client");
			return new PulsarClientProxy(this.customizer);
		}
		var clientBuilder = PulsarClient.builder();
		this.customizer.customize(clientBuilder);
		return clientBuilder.build();
	}

	@Override
	public void setEnvironment(Environment environment) {
		this.useRestartableClient = environment.getProperty("spring.pulsar.client.restartable", Boolean.class, true);
	}

}
