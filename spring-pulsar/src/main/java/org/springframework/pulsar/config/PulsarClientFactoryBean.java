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

package org.springframework.pulsar.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.pulsar.client.api.PulsarClient;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;

/**
 * {@link FactoryBean} implementation for the {@link PulsarClient}.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public class PulsarClientFactoryBean extends AbstractFactoryBean<PulsarClient> {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private final Map<String, Object> config = new HashMap<>();

	public PulsarClientFactoryBean(Map<String, Object> config) {
		Objects.requireNonNull(config, "Config map cannot be null");
		this.config.putAll(config);
	}

	@Override
	public Class<?> getObjectType() {
		return PulsarClient.class;
	}

	@Override
	protected PulsarClient createInstance() throws Exception {
		return PulsarClient.builder().loadConf(this.config).build();
	}

	@Override
	protected void destroyInstance(@Nullable PulsarClient instance) throws Exception {
		if (instance != null) {
			this.logger.info(() -> "Closing client " + instance);
			instance.close();
		}
	}

}
