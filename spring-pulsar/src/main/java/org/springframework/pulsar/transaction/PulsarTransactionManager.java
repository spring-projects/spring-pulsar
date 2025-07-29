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

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.Transaction;

import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.InvalidIsolationLevelException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

/**
 * Binds a {@link Transaction native Pulsar transaction} from the specified
 * {@code PulsarClient} to the current thread, allowing for one transaction per thread per
 * Pulsar client.
 * <p>
 * This transaction manager is not able to provide XA transactions, for example in order
 * to share transactions between messaging and database access.
 * <p>
 * Application code is required to retrieve the transactional Pulsar resources via
 * {@link PulsarTransactionUtils#obtainResourceHolder}. The {@link PulsarTemplate} will
 * auto-detect a thread-bound transaction and automatically participate in it.
 * <p>
 * Transaction synchronization is turned off by default, as this manager might be used
 * alongside a datastore-based Spring transaction manager such as the JDBC
 * {@code DataSourceTransactionManager}, which has stronger needs for synchronization.
 *
 * @author Chris Bono
 * @since 1.1.0
 */
public class PulsarTransactionManager extends AbstractPlatformTransactionManager
		implements PulsarAwareTransactionManager {

	private static final LogAccessor LOG = new LogAccessor(PulsarTransactionManager.class);

	private final PulsarClient pulsarClient;

	/**
	 * Create a new transaction manager.
	 * @param pulsarClient the pulsar client used to construct the backing Pulsar native
	 * transactions.
	 */
	public PulsarTransactionManager(PulsarClient pulsarClient) {
		Assert.notNull(pulsarClient, "pulsarClient must not be null");
		this.pulsarClient = pulsarClient;
		setTransactionSynchronization(SYNCHRONIZATION_NEVER);
	}

	@Override
	public PulsarClient getPulsarClient() {
		return this.pulsarClient;
	}

	@Override
	protected Object doGetTransaction() {
		var resourceHolder = TransactionSynchronizationManager.getResource(this.pulsarClient);
		PulsarTransactionObject txObject = new PulsarTransactionObject();
		txObject.setResourceHolder(cast(resourceHolder));
		return txObject;
	}

	@Override
	protected boolean isExistingTransaction(Object transaction) {
		var txObject = asPulsarTxObject(transaction);
		return (txObject.getResourceHolder() != null);
	}

	@Override
	protected void doBegin(Object transaction, TransactionDefinition definition) {
		if (definition.getIsolationLevel() != TransactionDefinition.ISOLATION_DEFAULT) {
			throw new InvalidIsolationLevelException("Apache Pulsar does not support an isolation level concept");
		}
		try {
			int timeoutSeconds = determineTimeout(definition);
			var resourceHolder = PulsarTransactionUtils.obtainResourceHolder(this.pulsarClient,
					timeoutSeconds != TransactionDefinition.TIMEOUT_DEFAULT ? Duration.ofSeconds(timeoutSeconds)
							: null);
			LOG.debug(() -> "Created Pulsar transaction on [%s]".formatted(resourceHolder.getTransaction()));
			resourceHolder.setSynchronizedWithTransaction(true);
			var txObject = asPulsarTxObject(transaction);
			txObject.setResourceHolder(resourceHolder);
		}
		catch (Exception ex) {
			throw new CannotCreateTransactionException("Could not create Pulsar transaction", ex);
		}
	}

	@Override
	protected Object doSuspend(Object transaction) {
		var txObject = asPulsarTxObject(transaction);
		txObject.setResourceHolder(null);
		return TransactionSynchronizationManager.unbindResource(this.pulsarClient);
	}

	@Override
	protected void doResume(Object transaction, Object suspendedResources) {
		TransactionSynchronizationManager.bindResource(this.pulsarClient, suspendedResources);
	}

	@Override
	protected void doCommit(DefaultTransactionStatus status) {
		asPulsarTxObject(status.getTransaction()).getResourceHolder().commit();
	}

	@Override
	protected void doRollback(DefaultTransactionStatus status) {
		asPulsarTxObject(status.getTransaction()).getResourceHolder().rollback();
	}

	@Override
	protected void doSetRollbackOnly(DefaultTransactionStatus status) {
		asPulsarTxObject(status.getTransaction()).getResourceHolder().setRollbackOnly();
	}

	@Override
	protected void doCleanupAfterCompletion(Object transaction) {
		var txObject = asPulsarTxObject(transaction);
		TransactionSynchronizationManager.unbindResource(this.pulsarClient);
		txObject.getResourceHolder().clear();
	}

	@SuppressWarnings("unchecked")
	private <X> X cast(Object raw) {
		return (X) raw;
	}

	private PulsarTransactionObject asPulsarTxObject(Object rawTxObject) {
		return PulsarTransactionObject.class.cast(rawTxObject);
	}

}
