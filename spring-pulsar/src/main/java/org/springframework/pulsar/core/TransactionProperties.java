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

package org.springframework.pulsar.core;

import java.time.Duration;

/**
 * Common transaction settings for components.
 *
 * @author Chris Bono
 * @since 1.1.0
 */
public class TransactionProperties {

	/**
	 * Whether the component supports transactions.
	 */
	private boolean enabled;

	/**
	 * Whether the component requires transactions.
	 */
	private boolean required;

	/**
	 * Duration representing the transaction timeout - null to use default timeout of the
	 * underlying transaction system, or none if timeouts are not supported.
	 */
	private Duration timeout;

	public boolean isEnabled() {
		return this.enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public boolean isRequired() {
		return this.required;
	}

	public void setRequired(boolean required) {
		this.required = required;
	}

	public Duration getTimeout() {
		return this.timeout;
	}

	public void setTimeout(Duration timeout) {
		this.timeout = timeout;
	}

}
