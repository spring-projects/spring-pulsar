/*
 * Copyright 2023-present the original author or authors.
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

package org.springframework.pulsar.transaction;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.pulsar.client.api.PulsarClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.transaction.support.TransactionSynchronization;

/**
 * Tests for {@link PulsarResourceSynchronization}.
 *
 * @author Chris Bono
 */
class PulsarResourceSynchronizationTests {

	private final PulsarClient pulsarClient = mock(PulsarClient.class);

	@Test
	void processResourceAfterCommitDoesCommitOnResourceHolder() {
		var holder = mock(PulsarResourceHolder.class);
		var sync = new PulsarResourceSynchronization(holder, pulsarClient);
		sync.processResourceAfterCommit(holder);
		verify(holder).commit();
	}

	@Test
	void afterCompletionDoesCommitOnHolderWhenTxnStatusIsCommitted() {
		var holder = mock(PulsarResourceHolder.class);
		var sync = new PulsarResourceSynchronization(holder, pulsarClient);
		sync.afterCompletion(TransactionSynchronization.STATUS_COMMITTED);
		verify(holder).commit();
	}

	@ParameterizedTest
	@ValueSource(ints = { TransactionSynchronization.STATUS_ROLLED_BACK, TransactionSynchronization.STATUS_UNKNOWN })
	void afterCompletionDoesRollbackOnHolderWhenTxnStatusIsNotCommitted(int status) {
		var holder = mock(PulsarResourceHolder.class);
		var sync = new PulsarResourceSynchronization(holder, pulsarClient);
		sync.afterCompletion(status);
		verify(holder).rollback();
	}

}
