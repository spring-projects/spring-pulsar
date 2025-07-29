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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionBuilder;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Tests for {@link PulsarTransactionUtils}.
 *
 * @author Chris Bono
 */
class PulsarTransactionUtilsTests {

	private PulsarClient pulsarClient = mock(PulsarClient.class);

	@Nested
	class InTransaction {

		@Test
		void whenNoResourceThenReturnsFalse() {
			assertThat(PulsarTransactionUtils.inTransaction(pulsarClient)).isFalse();
		}

		@Test
		void whenResourceThenReturnsTrue() {
			TransactionSynchronizationManager.bindResource(pulsarClient, "some-fake-txn-object");
			assertThat(PulsarTransactionUtils.inTransaction(pulsarClient)).isTrue();
		}

		@Nested
		class WithActualTransactionActive {

			// NOTE: Because this test sets the thread local 'actualTransactionActive'
			// which interferes w/ the other InTransaction tests it is nested so that it
			// executes after the other tests.
			@Test
			void whenNoResourceThenReturnsTrue() {
				TransactionSynchronizationManager.setActualTransactionActive(true);
				assertThat(PulsarTransactionUtils.inTransaction(pulsarClient)).isTrue();
			}

		}

	}

	@Nested
	class GetResourceHolder {

		@Test
		void whenNoResourceThenReturnsNull() {
			assertThat(PulsarTransactionUtils.getResourceHolder(pulsarClient)).isNull();
		}

		@Test
		void whenResourceThenReturnsResource() {
			var resourceHolder = new PulsarResourceHolder(mock(Transaction.class));
			TransactionSynchronizationManager.bindResource(pulsarClient, resourceHolder);
			assertThat(PulsarTransactionUtils.getResourceHolder(pulsarClient)).isEqualTo(resourceHolder);
		}

	}

	@Nested
	class ObtainResourceHolder {

		@Test
		void whenResourceThenReturnsResource() {
			var resourceHolder = new PulsarResourceHolder(mock(Transaction.class));
			TransactionSynchronizationManager.bindResource(pulsarClient, resourceHolder);
			assertThat(PulsarTransactionUtils.obtainResourceHolder(pulsarClient, null)).isEqualTo(resourceHolder);
		}

		@Test
		void whenNoResourceThenCreatesResourceWithoutTimeout() {
			var txn = mock(Transaction.class);
			var txnBuilder = mock(TransactionBuilder.class);
			when(txnBuilder.build()).thenReturn(CompletableFuture.completedFuture(txn));
			when(pulsarClient.newTransaction()).thenReturn(txnBuilder);
			assertThat(PulsarTransactionUtils.obtainResourceHolder(pulsarClient, null))
				.extracting(PulsarResourceHolder::getTransaction)
				.isEqualTo(txn);
			assertThat(PulsarTransactionUtils.getResourceHolder(pulsarClient))
				.extracting(PulsarResourceHolder::getTransaction)
				.isEqualTo(txn);
		}

		@Test
		void whenNoResourceThenCreatesResourceWithTimeout() {
			var txn = mock(Transaction.class);
			var txnBuilder = mock(TransactionBuilder.class);
			when(txnBuilder.build()).thenReturn(CompletableFuture.completedFuture(txn));
			when(pulsarClient.newTransaction()).thenReturn(txnBuilder);
			long nowEpochMillis = System.currentTimeMillis();
			var resourceHolder = PulsarTransactionUtils.obtainResourceHolder(pulsarClient, Duration.ofSeconds(60));
			assertThat(resourceHolder.getTransaction()).isEqualTo(txn);
			assertThat(resourceHolder.hasTimeout()).isTrue();
			long timeoutEpochMillis = resourceHolder.getDeadline().getTime();
			assertThat(nowEpochMillis + 60_000).isCloseTo(timeoutEpochMillis, Offset.offset(500L));
			verify(txnBuilder).withTransactionTimeout(61, TimeUnit.SECONDS);
		}

	}

	@Nested
	class Abort {

		@Test
		void whenTransactionIsNotNullThenTxnIsAborted() {
			var txn = mock(Transaction.class);
			when(txn.abort()).thenReturn(CompletableFuture.completedFuture(null));
			PulsarTransactionUtils.abort(txn);
			verify(txn).abort();
		}

		@Test
		void whenTransactionIsNullThenThrowsException() {
			assertThatIllegalArgumentException().isThrownBy(() -> PulsarTransactionUtils.abort(null));
		}

	}

}
