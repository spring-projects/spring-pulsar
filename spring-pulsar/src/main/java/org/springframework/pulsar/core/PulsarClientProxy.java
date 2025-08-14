/*
 * Copyright 2022-present the original author or authors.
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

package org.springframework.pulsar.core;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableViewBuilder;
import org.apache.pulsar.client.api.transaction.TransactionBuilder;

import org.springframework.context.Lifecycle;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.log.LogAccessor;
import org.springframework.util.Assert;

/**
 * A {@link PulsarClient} implementation that delegates to another actual Pulsar client.
 * The proxy client can be stopped and then started and still be in a usable state without
 * any knowledge of the restart or changes required to the users of the client.
 * <p>
 * The proxy client participates in the Spring {@link SmartLifecycle Lifecycle} and closes
 * the underlying client when {@link SmartLifecycle#stop() stopped} and creates another
 * delegate client when subsequently {@link SmartLifecycle#start() started}.
 *
 * @author Chris Bono
 */
final class PulsarClientProxy extends RestartableSingletonFactory<PulsarClient> implements PulsarClient {

	private static final int LIFECYCLE_PHASE = (Integer.MIN_VALUE / 2) - 200;

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private final PulsarClientBuilderCustomizer customizer;

	/**
	 * Construct a factory that creates clients using a customized Pulsar client builder.
	 * @param customizer the customizer to apply to the builder
	 */
	PulsarClientProxy(PulsarClientBuilderCustomizer customizer) {
		Assert.notNull(customizer, "customizer must not be null");
		this.customizer = customizer;
	}

	/**
	 * Return the phase that this lifecycle object is supposed to run in.
	 * <p>
	 * Lifecycle objects are started in ascending phase order (those w/ smaller phases are
	 * started before those with larger phases).
	 * <p>
	 * Lifecycle objects are stopped in descending phase order (those w/ larger phases are
	 * stopped before those with smaller phases).
	 * <p>
	 * The phases range from {@link Integer#MIN_VALUE} to {@link Integer#MAX_VALUE}.
	 * <p>
	 * The restartable client has a phase value that is roughly at the half-way marker on
	 * the left hand side of the phase continuum (in the middle of &quot;min&quot; and
	 * &quot;0&quot;). If another component depends on the restartable client it should
	 * use a phase that is greater than this value.
	 * <p>
	 * Because {@link Lifecycle regular} lifecycle objects have a default phase of
	 * &quot;0&quot; and {@link SmartLifecycle smart} lifecycle objects have a default
	 * phase of &quot;max&quot;, the restartable client will be started before (and
	 * stopped after) the majority of all other default configured lifecycle objects.
	 * @return the phase to execute in ({@link #LIFECYCLE_PHASE})
	 */
	@Override
	public int getPhase() {
		return LIFECYCLE_PHASE;
	}

	@Override
	protected PulsarClient createInstance() {
		this.logger.debug(() -> "Creating client");
		var clientBuilder = PulsarClient.builder();
		this.customizer.customize(clientBuilder);
		try {
			return clientBuilder.build();
		}
		catch (PulsarClientException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected void stopInstance(PulsarClient pulsarClient) {
		this.logger.debug(() -> "Closing client");
		try {
			pulsarClient.close();
		}
		catch (PulsarClientException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected boolean discardInstanceAfterStop() {
		return false;
	}

	// --- PulsarClient implementation below here

	@Override
	public ProducerBuilder<byte[]> newProducer() {
		return this.getRequiredInstance().newProducer();
	}

	@Override
	public <T> ProducerBuilder<T> newProducer(Schema<T> schema) {
		return this.getRequiredInstance().newProducer(schema);
	}

	@Override
	public ConsumerBuilder<byte[]> newConsumer() {
		return this.getRequiredInstance().newConsumer();
	}

	@Override
	public <T> ConsumerBuilder<T> newConsumer(Schema<T> schema) {
		return this.getRequiredInstance().newConsumer(schema);
	}

	@Override
	public ReaderBuilder<byte[]> newReader() {
		return this.getRequiredInstance().newReader();
	}

	@Override
	public <T> ReaderBuilder<T> newReader(Schema<T> schema) {
		return this.getRequiredInstance().newReader(schema);
	}

	@SuppressWarnings("deprecation")
	@Override
	public <T> TableViewBuilder<T> newTableViewBuilder(Schema<T> schema) {
		return this.getRequiredInstance().newTableViewBuilder(schema);
	}

	@Override
	public TableViewBuilder<byte[]> newTableView() {
		return this.getRequiredInstance().newTableView();
	}

	@Override
	public <T> TableViewBuilder<T> newTableView(Schema<T> schema) {
		return this.getRequiredInstance().newTableView(schema);
	}

	@Override
	public void updateServiceUrl(String serviceUrl) throws PulsarClientException {
		this.getRequiredInstance().updateServiceUrl(serviceUrl);
	}

	@Override
	public CompletableFuture<List<String>> getPartitionsForTopic(String topic, boolean metadataAutoCreationEnabled) {
		return this.getRequiredInstance().getPartitionsForTopic(topic, metadataAutoCreationEnabled);
	}

	@Override
	public void close() throws PulsarClientException {
		this.getRequiredInstance().close();
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		return this.getRequiredInstance().closeAsync();
	}

	@Override
	public void shutdown() throws PulsarClientException {
		this.getRequiredInstance().shutdown();
	}

	@Override
	public boolean isClosed() {
		return this.getRequiredInstance().isClosed();
	}

	@Override
	public TransactionBuilder newTransaction() {
		return this.getRequiredInstance().newTransaction();
	}

}
