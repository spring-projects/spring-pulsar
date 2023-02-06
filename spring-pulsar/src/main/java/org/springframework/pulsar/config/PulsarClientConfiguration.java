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

import org.springframework.util.Assert;

/**
 * Configuration for the Pulsar client.
 *
 * @author Soby Chacko
 */
public class PulsarClientConfiguration {

	private final Map<String, Object> configs = new HashMap<>();

	public PulsarClientConfiguration() {
		this(Map.of("serviceUrl", "pulsar://localhost:6650"));
	}

	public PulsarClientConfiguration(Map<String, Object> configs) {
		Assert.notNull(configs, "Configuration map cannot be null");
		this.configs.putAll(configs);
		this.configs.putIfAbsent("serviceUrl", "pulsar://localhost:6650");
	}

	public Map<String, Object> getConfigs() {
		return this.configs;
	}

}
