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

package org.springframework.pulsar.function;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.jspecify.annotations.Nullable;

import org.springframework.pulsar.PulsarException;

/**
 * Represents a user-defined Pulsar Function backed by a {@link FunctionConfig}.
 *
 * @param config the function details
 * @param stopPolicy the action to take on the function when the server is stopped
 * @param updateOptions the options to use during an update operation (optional)
 * @author Chris Bono
 */
public record PulsarFunction(FunctionConfig config, FunctionStopPolicy stopPolicy,
		@Nullable UpdateOptions updateOptions) implements PulsarFunctionOperations<FunctionConfig> {

	public PulsarFunction(FunctionConfig config, @Nullable UpdateOptions updateOptions) {
		this(config, FunctionStopPolicy.DELETE, updateOptions);
	}

	@Override
	public String name() {
		return config().getName();
	}

	@Override
	public FunctionType type() {
		return FunctionType.FUNCTION;
	}

	@Override
	public String archive() {
		return config().getJar();
	}

	@Override
	public FunctionConfig get(PulsarAdmin admin) throws PulsarAdminException {
		return admin.functions().getFunction(config().getTenant(), config().getNamespace(), config().getName());
	}

	@Override
	public void updateWithUrl(PulsarAdmin admin) throws PulsarAdminException {
		admin.functions().updateFunctionWithUrl(config(), archive(), updateOptions());
	}

	@Override
	public void update(PulsarAdmin admin) throws PulsarAdminException {
		admin.functions().updateFunction(config(), archive(), updateOptions());
	}

	@Override
	public void createWithUrl(PulsarAdmin admin) throws PulsarAdminException {
		admin.functions().createFunctionWithUrl(config(), archive());
	}

	@Override
	public void create(PulsarAdmin admin) throws PulsarAdminException {
		admin.functions().createFunction(config(), archive());
	}

	@Override
	public void stop(PulsarAdmin admin) {
		try {
			admin.functions().stopFunction(config().getTenant(), config().getNamespace(), config().getName());
		}
		catch (PulsarAdminException e) {
			throw new PulsarException(e.getMessage(), e);
		}
	}

	@Override
	public void delete(PulsarAdmin admin) {
		try {
			admin.functions().deleteFunction(config().getTenant(), config().getNamespace(), config().getName());
		}
		catch (PulsarAdminException e) {
			throw new PulsarException(e.getMessage(), e);
		}
	}
}
