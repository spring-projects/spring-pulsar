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

import org.apache.pulsar.client.api.PulsarClient;

import org.springframework.transaction.support.ResourceHolderSynchronization;
import org.springframework.transaction.support.TransactionSynchronization;

/**
 * Callback for resource cleanup at the end of a Spring transaction.
 *
 * @author Chris Bono
 */
class PulsarResourceSynchronization extends ResourceHolderSynchronization<PulsarResourceHolder, PulsarClient> {

	private final PulsarResourceHolder resourceHolder;

	PulsarResourceSynchronization(PulsarResourceHolder resourceHolder, PulsarClient resourceKey) {
		super(resourceHolder, resourceKey);
		this.resourceHolder = resourceHolder;
	}

	@Override
	protected boolean shouldReleaseBeforeCompletion() {
		return false;
	}

	@Override
	protected void processResourceAfterCommit(PulsarResourceHolder resourceHolder) {
		resourceHolder.commit();
	}

	@Override
	public void afterCompletion(int status) {
		try {
			if (status == TransactionSynchronization.STATUS_COMMITTED) {
				this.resourceHolder.commit();
			}
			else {
				this.resourceHolder.rollback();
			}
		}
		finally {
			super.afterCompletion(status);
		}
	}

}
