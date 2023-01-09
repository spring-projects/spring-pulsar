/*
 * Copyright 2023-2023 the original author or authors.
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

import javax.annotation.Nullable;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.io.SinkConfig;

/**
 * Represents a Pulsar Sink backed by a {@link SinkConfig}.
 * @param config the sink details
 * @param updateOptions the options to use during an update operation (optional)
 *
 * @author Chris Bono
 */
public record PulsarSink(SinkConfig config,
		@Nullable UpdateOptions updateOptions) implements PulsarFunctionOperations<SinkConfig> {

	@Override
	public String name() {
		return config().getName();
	}

	@Override
	public FunctionType type() {
		return FunctionType.SINK;
	}

	@Override
	public String archive() {
		return config().getArchive();
	}

	@Override
	public SinkConfig get(PulsarAdmin admin) throws PulsarAdminException {
		return admin.sinks().getSink(config().getTenant(), config().getNamespace(), config().getName());
	}

	@Override
	public void updateWithUrl(PulsarAdmin admin) throws PulsarAdminException {
		admin.sinks().updateSinkWithUrl(config(), archive(), updateOptions());
	}

	@Override
	public void update(PulsarAdmin admin) throws PulsarAdminException {
		admin.sinks().updateSink(config(), archive(), updateOptions());
	}

	@Override
	public void createWithUrl(PulsarAdmin admin) throws PulsarAdminException {
		admin.sinks().createSinkWithUrl(config(), archive());
	}

	@Override
	public void create(PulsarAdmin admin) throws PulsarAdminException {
		admin.sinks().createSink(config(), archive());
	}
}
