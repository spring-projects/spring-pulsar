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

package org.springframework.pulsar.autoconfigure;

import org.apache.pulsar.client.api.PulsarClient;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.pulsar.core.reactive.DefaultReactivePulsarSenderFactory;
import org.springframework.pulsar.core.reactive.ReactivePulsarSenderFactory;
import org.springframework.pulsar.core.reactive.ReactivePulsarSenderTemplate;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for Apache Pulsar.
 *
 * @author Soby Chacko
 * @author Chris Bono
 * @author Alexander Preuß
 */
@AutoConfiguration(after = PulsarAutoConfiguration.class)
@ConditionalOnClass(ReactivePulsarSenderTemplate.class)
@EnableConfigurationProperties(PulsarProperties.class)
public class PulsarReactiveAutoConfiguration {

	private final PulsarProperties properties;

	public PulsarReactiveAutoConfiguration(PulsarProperties properties) {
		this.properties = properties;
	}

	@Bean
	@ConditionalOnMissingBean(ReactivePulsarSenderTemplate.class)
	public ReactivePulsarSenderTemplate<?> pulsarReactiveSenderTemplate(
			ReactivePulsarSenderFactory<?> reactivePulsarSenderFactory) {
		return new ReactivePulsarSenderTemplate(reactivePulsarSenderFactory);
	}

	@Bean
	@ConditionalOnMissingBean(ReactivePulsarSenderFactory.class)
	public ReactivePulsarSenderFactory<?> reactivePulsarSenderFactory(PulsarClient pulsarClient) {
		return new DefaultReactivePulsarSenderFactory(pulsarClient, null); //, this.properties.buildReactiveMessageSenderSpec());
	}
}
