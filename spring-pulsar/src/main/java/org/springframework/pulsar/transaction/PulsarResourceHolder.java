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

import org.apache.pulsar.client.api.transaction.Transaction;

import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.PulsarException;
import org.springframework.transaction.support.ResourceHolderSupport;
import org.springframework.util.Assert;

/**
 * Resource holder for a native Pulsar transaction object which is the transactional
 * resource when handling transactions for Spring Pulsar.
 *
 * @author Chris Bono
 * @since 1.1.0
 */
public class PulsarResourceHolder extends ResourceHolderSupport {

	private static final LogAccessor LOG = new LogAccessor(PulsarResourceHolder.class);

	private final Transaction transaction;

	private boolean committed;

	public PulsarResourceHolder(Transaction transaction) {
		Assert.notNull(transaction, "transaction must not be null");
		this.transaction = transaction;
	}

	public Transaction getTransaction() {
		return this.transaction;
	}

	public void commit() {
		if (!this.committed) {
			LOG.trace(() -> "Committing Pulsar txn [%s]...".formatted(this.transaction));
			try {
				this.transaction.commit().get();
			}
			catch (Exception e) {
				throw PulsarException.unwrap(e);
			}
			LOG.trace(() -> "Committed Pulsar txn [%s]".formatted(this.transaction));
			this.committed = true;
		}
		else {
			LOG.trace(() -> "Skipping request to commit - already committed");
		}
	}

	public void rollback() {
		LOG.trace(() -> "Rolling back Pulsar txn [%s]...".formatted(this.transaction));
		PulsarTransactionUtils.abort(this.transaction);
	}

	@Override
	public String toString() {
		return "PulsarResourceHolder{transaction=%s, committed=%s}".formatted(this.transaction, this.committed);
	}

}
