/*
 * Copyright 2024-2024 the original author or authors.
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

import org.springframework.transaction.support.SmartTransactionObject;

/**
 * A transaction object representing a native {@link Transaction Pulsar transaction}. Used
 * as transaction object by {@code PulsarTransactionManager}.
 *
 * @author Chris Bono
 * @since 1.1.0
 */
class PulsarTransactionObject implements SmartTransactionObject {

	private PulsarResourceHolder resourceHolder;

	PulsarTransactionObject() {
	}

	public PulsarResourceHolder getResourceHolder() {
		return this.resourceHolder;
	}

	public void setResourceHolder(PulsarResourceHolder resourceHolder) {
		this.resourceHolder = resourceHolder;
	}

	@Override
	public boolean isRollbackOnly() {
		return this.resourceHolder.isRollbackOnly();
	}

}
