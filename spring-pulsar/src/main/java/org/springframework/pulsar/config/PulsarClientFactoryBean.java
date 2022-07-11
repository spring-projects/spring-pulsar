/*
 * Copyright 2022 the original author or authors.
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

import org.apache.commons.logging.LogFactory;
import org.apache.pulsar.client.api.PulsarClient;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.core.log.LogAccessor;

/**
 * {@link FactoryBean} implementation for the {@link PulsarClient}.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public class PulsarClientFactoryBean extends AbstractFactoryBean<PulsarClient> {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(this.getClass()));

	private final PulsarClientConfiguration pulsarClientConfiguration;

	public PulsarClientFactoryBean(PulsarClientConfiguration pulsarClientConfiguration) {
		this.pulsarClientConfiguration = pulsarClientConfiguration;
	}

	@Override
	public Class<?> getObjectType() {
		return PulsarClient.class;
	}

	@Override
	protected PulsarClient createInstance() throws Exception {
		return PulsarClient.builder()
				.loadConf(this.pulsarClientConfiguration.getConfigs())
				.build();
	}

	@Override
	protected void destroyInstance(PulsarClient instance) throws Exception {
		if (instance != null) {
			this.logger.info(() -> "Closing client " + instance);
			instance.close();
		}
	}

}
