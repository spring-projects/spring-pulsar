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

package org.springframework.pulsar.spring.cloud.stream.binder.properties;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.binder.AbstractExtendedBindingProperties;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;

@ConfigurationProperties("spring.cloud.stream.pulsar")
public class PulsarExtendedBindingProperties extends
		AbstractExtendedBindingProperties<PulsarConsumerProperties, PulsarProducerProperties, PulsarBindingProperties> {

	private static final String DEFAULTS_PREFIX = "spring.cloud.stream.pulsar.default";

	@Override
	public String getDefaultsPrefix() {
		return DEFAULTS_PREFIX;
	}

	@Override
	public Map<String, PulsarBindingProperties> getBindings() {
		return this.doGetBindings();
	}

	@Override
	public Class<? extends BinderSpecificPropertiesProvider> getExtendedPropertiesEntryClass() {
		return PulsarBindingProperties.class;
	}

}
