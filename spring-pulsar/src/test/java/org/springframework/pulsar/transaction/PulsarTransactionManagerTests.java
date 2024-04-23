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

package org.springframework.pulsar.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.pulsar.client.api.PulsarClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Tests for {@link PlatformTransactionManager}.
 *
 * @author Chris Bono
 */
class PulsarTransactionManagerTests {

	private PulsarClient pulsarClient = mock(PulsarClient.class);

	private PulsarTransactionManager transactionManager;

	private PulsarResourceHolder resourceHolder;

	private PulsarTransactionObject transactionObject;

	private DefaultTransactionStatus transactionStatus;

	@BeforeEach
	void prepareForTest() {
		transactionManager = new PulsarTransactionManager(pulsarClient);
		resourceHolder = mock(PulsarResourceHolder.class);
		transactionObject = new PulsarTransactionObject();
		transactionObject.setResourceHolder(resourceHolder);
		transactionStatus = mock(DefaultTransactionStatus.class);
		when(transactionStatus.getTransaction()).thenReturn(transactionObject);
	}

	@Test
	void doGetTransactionReturnsPulsarTxnObject() {
		TransactionSynchronizationManager.bindResource(this.pulsarClient, resourceHolder);
		assertThat(transactionManager.doGetTransaction()).isInstanceOf(PulsarTransactionObject.class)
			.hasFieldOrPropertyWithValue("resourceHolder", resourceHolder);
	}

	@Test
	void isExistingTransactionReturnsTrueWhenTxnObjectHasResourceHolder() {
		var txnObject = new PulsarTransactionObject();
		txnObject.setResourceHolder(resourceHolder);
		assertThat(transactionManager.isExistingTransaction(txnObject)).isTrue();
	}

	@Test
	void isExistingTransactionReturnsFalseWhenTxnObjectHasNoResourceHolder() {
		var txnObject = new PulsarTransactionObject();
		assertThat(transactionManager.isExistingTransaction(txnObject)).isFalse();
	}

	@Test
	void doSuspendUnbindsAndNullsOutResourceHolder() {
		TransactionSynchronizationManager.bindResource(this.pulsarClient, resourceHolder);
		transactionManager.doSuspend(transactionObject);
		assertThat(transactionObject.getResourceHolder()).isNull();
		assertThat(TransactionSynchronizationManager.getResource(this.pulsarClient)).isNull();
	}

	@Test
	void doResumeBindsResourceHolder() {
		transactionManager.doResume("unused", resourceHolder);
		assertThat(TransactionSynchronizationManager.getResource(this.pulsarClient)).isSameAs(resourceHolder);
	}

	@Test
	void doCommitDoesCommitOnResourceHolder() {
		transactionManager.doCommit(transactionStatus);
		verify(resourceHolder).commit();
	}

	@Test
	void doRollbackDoesRollbackOnResourceHolder() {
		transactionManager.doRollback(transactionStatus);
		verify(resourceHolder).rollback();
	}

	@Test
	void doSetRollbackOnlyDoesSetRollbackOnlyOnResourceHolder() {
		transactionManager.doSetRollbackOnly(transactionStatus);
		verify(resourceHolder).setRollbackOnly();
	}

	@Test
	void doCleanupDoesUnbindAndClearResourceHolder() {
		TransactionSynchronizationManager.bindResource(this.pulsarClient, resourceHolder);
		transactionManager.doCleanupAfterCompletion(transactionObject);
		assertThat(TransactionSynchronizationManager.getResource(this.pulsarClient)).isNull();
		verify(transactionObject.getResourceHolder()).clear();
	}

}
