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
import org.jspecify.annotations.Nullable;

import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.util.Assert;

/**
 * A transaction object representing a native {@link Transaction Pulsar transaction}. Used
 * as transaction object by {@code PulsarTransactionManager}.
 *
 * @author Chris Bono
 * @since 1.1.0
 */
class PulsarTransactionObject implements SmartTransactionObject {

	private @Nullable PulsarResourceHolder resourceHolder;

	PulsarTransactionObject() {
	}

	public PulsarResourceHolder getRequiredResourceHolder() {
		Assert.notNull(this.resourceHolder, () -> "resourceHolder required but was null");
		return this.resourceHolder;
	}

	@Nullable public PulsarResourceHolder getResourceHolder() {
		return this.resourceHolder;
	}

	public void setResourceHolder(@Nullable PulsarResourceHolder resourceHolder) {
		this.resourceHolder = resourceHolder;
	}

	@Override
	public boolean isRollbackOnly() {
		return this.getRequiredResourceHolder().isRollbackOnly();
	}

}
