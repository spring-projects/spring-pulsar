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
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.io.SourceConfig;
import org.jspecify.annotations.Nullable;

import org.springframework.pulsar.PulsarException;

/**
 * Represents a Pulsar Source backed by a {@link SourceConfig}.
 *
 * @param config the source details
 * @param stopPolicy the action to take on the source when the server is stopped
 * @param updateOptions the options to use during an update operation (optional)
 * @author Chris Bono
 */
public record PulsarSource(SourceConfig config, FunctionStopPolicy stopPolicy,
		@Nullable UpdateOptions updateOptions) implements PulsarFunctionOperations<SourceConfig> {

	public PulsarSource(SourceConfig config, @Nullable UpdateOptions updateOptions) {
		this(config, FunctionStopPolicy.DELETE, updateOptions);
	}

	@Override
	public String name() {
		return config().getName();
	}

	@Override
	public FunctionType type() {
		return FunctionType.SOURCE;
	}

	@Override
	public String archive() {
		return config().getArchive();
	}

	@Override
	public SourceConfig get(PulsarAdmin admin) throws PulsarAdminException {
		return admin.sources().getSource(config().getTenant(), config().getNamespace(), config().getName());
	}

	@Override
	public void updateWithUrl(PulsarAdmin admin) throws PulsarAdminException {
		admin.sources().updateSourceWithUrl(config(), archive(), updateOptions());
	}

	@Override
	public void update(PulsarAdmin admin) throws PulsarAdminException {
		admin.sources().updateSource(config(), archive(), updateOptions());
	}

	@Override
	public void createWithUrl(PulsarAdmin admin) throws PulsarAdminException {
		admin.sources().createSourceWithUrl(config(), archive());
	}

	@Override
	public void create(PulsarAdmin admin) throws PulsarAdminException {
		admin.sources().createSource(config(), archive());
	}

	@Override
	public void stop(PulsarAdmin admin) {
		try {
			admin.sources().stopSource(config().getTenant(), config().getNamespace(), config().getName());
		}
		catch (PulsarAdminException e) {
			throw new PulsarException(e.getMessage(), e);
		}
	}

	@Override
	public void delete(PulsarAdmin admin) {
		try {
			admin.sources().deleteSource(config().getTenant(), config().getNamespace(), config().getName());
		}
		catch (PulsarAdminException e) {
			throw new PulsarException(e.getMessage(), e);
		}
	}
}
