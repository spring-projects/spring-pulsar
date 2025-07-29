/*
 * Copyright 2024-present the original author or authors.
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

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.Transaction;

import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.pulsar.PulsarException;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

/**
 * Provides conveniences for managing Pulsar transactions.
 *
 * @author Chris Bono
 * @since 1.1.0
 */
public final class PulsarTransactionUtils {

	private static final LogAccessor LOG = new LogAccessor(PulsarTransactionUtils.class);

	private PulsarTransactionUtils() {
	}

	/**
	 * Determine if the given pulsar client is currently participating in an active
	 * transaction. The result will be {@code true} if the current thread is associated
	 * with an actual transaction or if the given pulsar client is already synchronized
	 * with the current transaction.
	 * @param pulsarClient the client to check
	 * @return whether the client is currently participating in an active transaction
	 */
	public static boolean inTransaction(PulsarClient pulsarClient) {
		return TransactionSynchronizationManager.getResource(pulsarClient) != null
				|| TransactionSynchronizationManager.isActualTransactionActive();
	}

	/**
	 * Aborts a Pulsar transaction asynchronously, logging the outcome at trace level.
	 * @param transaction the transaction to abort
	 */
	public static void abort(Transaction transaction) {
		Assert.notNull(transaction, "transaction must not be null");
		LOG.trace(() -> "Aborting Pulsar txn [%s]...".formatted(transaction));
		transaction.abort().whenComplete((__, ex) -> {
			if (ex != null) {
				LOG.error(ex,
						() -> "Failed to abort Pulsar txn [%s] due to: %s".formatted(transaction, ex.getMessage()));
			}
			else {
				LOG.trace(() -> "Completed abort of Pulsar txn [%s]".formatted(transaction));
			}
		});
	}

	/**
	 * Get a resource holder that is already synchronized with the current transaction.
	 * @param pulsarClient the client used to obtain the transaction resource
	 * @return the resource holder
	 */
	@Nullable
	public static PulsarResourceHolder getResourceHolder(PulsarClient pulsarClient) {
		return (PulsarResourceHolder) TransactionSynchronizationManager.getResource(pulsarClient);
	}

	/**
	 * Obtain a resource holder that is synchronized with the current transaction. If
	 * there is already one associated with the current transaction it is returned
	 * otherwise a new one is created and associated to the current transaction.
	 * @param pulsarClient the client used to obtain the transaction resource
	 * @param timeout the max time to wait for the transaction to be completed before it
	 * will be aborted or null to use defaults
	 * @return the resource holder
	 * @since 1.1.0
	 */
	public static PulsarResourceHolder obtainResourceHolder(PulsarClient pulsarClient, @Nullable Duration timeout) {
		Assert.notNull(pulsarClient, "pulsarClient must not be null");
		var resourceHolder = getResourceHolder(pulsarClient);
		if (resourceHolder != null) {
			LOG.trace(() -> "Found already bound Pulsar txn resource " + resourceHolder);
			return resourceHolder;
		}
		var pulsarTxn = createPulsarTransaction(pulsarClient, timeout);
		var newResourceHolder = new PulsarResourceHolder(pulsarTxn);
		LOG.trace(() -> "Created Pulsar txn resource " + newResourceHolder);
		if (timeout != null) {
			newResourceHolder.setTimeoutInSeconds(Math.toIntExact(timeout.toSeconds()));
		}
		bindResourceToTransaction(pulsarClient, newResourceHolder);
		return newResourceHolder;
	}

	private static Transaction createPulsarTransaction(PulsarClient pulsarClient, @Nullable Duration timeout) {
		try {
			var txnBuilder = pulsarClient.newTransaction();
			if (timeout != null) {
				// bump Spring timeout by 1s so native Pulsar txn does not expire first
				txnBuilder.withTransactionTimeout(timeout.toSeconds() + 1, TimeUnit.SECONDS);
			}
			return txnBuilder.build().get();
		}
		catch (Exception ex) {
			throw PulsarException.unwrap(ex);
		}
	}

	private static <K, V> void bindResourceToTransaction(PulsarClient pulsarClient,
			PulsarResourceHolder resourceHolder) {
		TransactionSynchronizationManager.bindResource(pulsarClient, resourceHolder);
		resourceHolder.setSynchronizedWithTransaction(true);
		if (TransactionSynchronizationManager.isSynchronizationActive()) {
			TransactionSynchronizationManager
				.registerSynchronization(new PulsarResourceSynchronization(resourceHolder, pulsarClient));
			LOG.debug(() -> "Registered synchronization for Pulsar txn resource " + resourceHolder);
		}
	}

}
