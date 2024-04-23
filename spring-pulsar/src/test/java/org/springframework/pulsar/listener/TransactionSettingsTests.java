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

package org.springframework.pulsar.listener;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import org.springframework.pulsar.listener.PulsarContainerProperties.TransactionSettings;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionDefinition;

/**
 * Unit tests for {@link TransactionSettings}.
 *
 * @author Chris Bono
 */
class TransactionSettingsTests {

	@Test
	void whenTimeoutNotSetThenReturnsConfiguredDefinition() {
		var txnSettings = new TransactionSettings();
		var txnDefinition = new DefaultTransactionDefinition();
		txnSettings.setTransactionDefinition(txnDefinition);
		assertThat(txnSettings.determineTransactionDefinition()).isSameAs(txnDefinition);
	}

	@Test
	void whenTimeoutSetButDefinitionNotSetThenReturnsNewDefinitionWithTimeout() {
		var txnSettings = new TransactionSettings();
		txnSettings.setTimeout(Duration.ofSeconds(100));
		assertThat(txnSettings.determineTransactionDefinition()).extracting(TransactionDefinition::getTimeout)
			.isEqualTo(100);
	}

	@Test
	void whenTimeoutSetAndDefinitionSetThenReturnsCloneDefinitionUpdatedWithTimeout() {
		var txnSettings = new TransactionSettings();
		txnSettings.setTimeout(Duration.ofSeconds(200));
		var txnDefinition = new DefaultTransactionDefinition();
		txnDefinition.setTimeout(100);
		txnSettings.setTransactionDefinition(txnDefinition);
		assertThat(txnSettings.determineTransactionDefinition()).extracting(TransactionDefinition::getTimeout)
			.isEqualTo(200);
	}

}
