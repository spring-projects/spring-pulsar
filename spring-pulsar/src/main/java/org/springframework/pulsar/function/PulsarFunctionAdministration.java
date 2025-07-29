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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.functions.Utils;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.PulsarException;
import org.springframework.pulsar.core.PulsarAdministration;

/**
 * Responsible for creating and updating any user-defined Pulsar functions, sinks, or
 * sources.
 *
 * @author Chris Bono
 */
public class PulsarFunctionAdministration implements SmartLifecycle {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private final PulsarAdministration pulsarAdministration;

	private final ObjectProvider<PulsarFunction> pulsarFunctions;

	private final ObjectProvider<PulsarSink> pulsarSinks;

	private final ObjectProvider<PulsarSource> pulsarSources;

	private final List<PulsarFunctionOperations<?>> processedFunctions;

	private final boolean failFast;

	private final boolean propagateFailures;

	private final boolean propagateStopFailures;

	private volatile boolean running;

	/**
	 * Construct a {@code PulsarFunctionAdministration} instance.
	 * @param pulsarAdministration the pulsar admin to make the API calls with
	 * @param pulsarFunctions provider of functions to create/update
	 * @param pulsarSinks provider of sinks to create/update
	 * @param pulsarSources provider of sources to create/update
	 * @param failFast whether to stop processing when a failure occurs
	 * @param propagateFailures whether to throw an exception when a failure occurs during
	 * server startup while creating/updating functions
	 * @param propagateStopFailures whether to throw an exception when a failure occurs
	 * during server shutdown while enforcing stop policy on functions
	 */
	public PulsarFunctionAdministration(PulsarAdministration pulsarAdministration,
			ObjectProvider<PulsarFunction> pulsarFunctions, ObjectProvider<PulsarSink> pulsarSinks,
			ObjectProvider<PulsarSource> pulsarSources, boolean failFast, boolean propagateFailures,
			boolean propagateStopFailures) {
		this.pulsarAdministration = pulsarAdministration;
		this.pulsarFunctions = pulsarFunctions;
		this.pulsarSinks = pulsarSinks;
		this.pulsarSources = pulsarSources;
		this.failFast = failFast;
		this.propagateFailures = propagateFailures;
		this.propagateStopFailures = propagateStopFailures;
		this.processedFunctions = new ArrayList<>();
	}

	@Override
	public synchronized void start() {
		if (!this.running) {
			this.logger.debug(() -> "Processing Pulsar Functions");
			long start = System.currentTimeMillis();
			this.createOrUpdateUserDefinedFunctions();
			this.running = true;
			long duration = System.currentTimeMillis() - start;
			this.logger.debug(() -> "Processed Pulsar Functions in " + duration + " ms");
		}
	}

	@Override
	public synchronized void stop() {
		if (this.running) {
			this.logger.debug(() -> "Enforcing stop policy on Pulsar Functions");
			this.running = false;
			long start = System.currentTimeMillis();
			this.enforceStopPolicyOnUserDefinedFunctions();
			long duration = System.currentTimeMillis() - start;
			this.logger.debug(() -> "Enforced stop policy on Pulsar Functions in " + duration + " ms");
		}
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	/**
	 * Called during server startup, creates or updates any Pulsar functions registered by
	 * the application.
	 *
	 * <p>
	 * The functions, sinks, and sources are processed serially (in that order) as
	 * follows:
	 * <ul>
	 * <li>A create or update operation is performed depending on whether or not the
	 * function already exists.
	 * <li>If the operation fails the {@link #failFast} property controls whether
	 * processing should stop (fail fast) or continue on w/ the next function.
	 * </ul>
	 *
	 * <p>
	 * Once processing is complete, any failures are either logged or thrown to the caller
	 * (propagated) dependent on the {@link #propagateFailures} property.
	 * @throws PulsarFunctionException containing processing errors if the
	 * {@code propagateFailures} property is set to {@code true}
	 */
	public void createOrUpdateUserDefinedFunctions() {
		// Concat the functions/sinks/sources into a single stream
		Stream<PulsarFunctionOperations<?>> allFunctions = Stream.concat(
				Stream.concat(this.pulsarFunctions.orderedStream(), this.pulsarSinks.orderedStream()),
				this.pulsarSources.orderedStream());
		List<PulsarFunctionOperations<?>> functionsToProcess = allFunctions.toList();
		if (functionsToProcess.isEmpty()) {
			this.logger.debug("No user defined functions to process.");
			return;
		}

		try (PulsarAdmin admin = this.pulsarAdministration.createAdminClient()) {
			Map<PulsarFunctionOperations<?>, Exception> failures = new LinkedHashMap<>();
			for (PulsarFunctionOperations<?> function : functionsToProcess) {
				Optional<Exception> failure = createOrUpdateFunction(function, admin);
				if (failure.isEmpty()) {
					this.processedFunctions.add(function);
				}
				else {
					failures.put(function, failure.get());
					if (this.failFast) {
						break;
					}
				}
			}

			// Handle failures accordingly
			if (!failures.isEmpty()) {
				String msg = "Encountered " + failures.size() + " error(s) creating/updating functions: " + failures;
				if (this.propagateFailures) {
					throw new PulsarFunctionException(msg, failures);
				}
				this.logger.error(() -> msg);
			}
		}
		catch (PulsarClientException ex) {
			String msg = "Unable to create/update functions - could not create PulsarAdmin: " + ex.getMessage();
			if (this.propagateFailures) {
				throw new PulsarException(msg, ex);
			}
			this.logger.error(ex, () -> msg);
		}
	}

	private Optional<Exception> createOrUpdateFunction(PulsarFunctionOperations<?> function, PulsarAdmin admin) {
		try {
			// Use url api for 'http|file|source|sink|function'
			String archive = function.archive();
			boolean usePackageUrl = Utils.isFunctionPackageUrlSupported(archive);
			if (function.functionExists(admin)) {
				if (usePackageUrl) {
					this.logger.info(() -> buildLogMsg(function, true, true));
					function.updateWithUrl(admin);
				}
				else {
					this.logger.info(() -> buildLogMsg(function, true, false));
					function.update(admin);
				}
			}
			else {
				if (usePackageUrl) {
					this.logger.info(() -> buildLogMsg(function, false, true));
					function.createWithUrl(admin);
				}
				else {
					this.logger.info(() -> buildLogMsg(function, false, false));
					function.create(admin);
				}
			}
			return Optional.empty();
		}
		catch (PulsarAdminException ex) {
			if (ex.getStatusCode() == 400 && "Update contains no change".equals(ex.getHttpError())) {
				this.logger.debug(() -> "Update contained no change for " + functionDesc(function));
				return Optional.empty();
			}
			return Optional.of(ex);
		}
		catch (Exception ex) {
			return Optional.of(ex);
		}
	}

	private String buildLogMsg(PulsarFunctionOperations<?> function, boolean isUpdate, boolean isUrlArchive) {
		// <verb> '<name>' <type> (using (url|local) archive: <archive>
		// Ex: Updating 'Uppercase' function (using url archive: sink://foo.bar)
		return "%s %s (using %s archive: %s)".formatted(isUpdate ? "Updating" : "Creating", functionDesc(function),
				isUrlArchive ? "url" : "local", function.archive());
	}

	// VisibleForTesting
	List<PulsarFunctionOperations<?>> getProcessedFunctions() {
		return this.processedFunctions;
	}

	/**
	 * Called during server shutdown, enforces the stop policy on all Pulsar functions
	 * that were successfully processed during server startup.
	 *
	 * <p>
	 * The functions, sinks, and sources are processed in reverse startup order as
	 * follows:
	 * <ul>
	 * <li>The stop policy of each function is used to determine if the function should be
	 * stopped, removed, or left alone.
	 * </ul>
	 *
	 * <p>
	 * Once processing is complete, any failures are either logged or thrown to the caller
	 * (propagated) dependent on the {@link #propagateStopFailures} property.
	 * @throws PulsarFunctionException containing processing errors if the
	 * {@code propagateStopFailures} property is set to {@code true}
	 */
	public void enforceStopPolicyOnUserDefinedFunctions() {
		if (this.processedFunctions.isEmpty()) {
			this.logger.debug("No processed functions to enforce stop policy on");
			return;
		}

		try (PulsarAdmin admin = this.pulsarAdministration.createAdminClient()) {
			Map<PulsarFunctionOperations<?>, Exception> failures = new LinkedHashMap<>();
			// Spin through the processed functions in reverse startup order
			Collections.reverse(this.processedFunctions);
			for (PulsarFunctionOperations<?> function : this.processedFunctions) {
				Optional<Exception> failure = enforceStopPolicyOnFunction(function, admin);
				failure.ifPresent(e -> failures.put(function, e));
			}

			// Handle failures accordingly
			if (!failures.isEmpty()) {
				String msg = "Encountered " + failures.size() + " error(s) enforcing stop policy on functions: "
						+ failures;
				if (this.propagateStopFailures) {
					throw new PulsarFunctionException(msg, failures);
				}
				this.logger.error(() -> msg);
			}
		}
		catch (PulsarClientException ex) {
			String msg = "Unable to enforce stop policy on functions - could not create PulsarAdmin: "
					+ ex.getMessage();
			if (this.propagateStopFailures) {
				throw new PulsarException(msg, ex);
			}
			this.logger.error(ex, () -> msg);
		}
	}

	private Optional<Exception> enforceStopPolicyOnFunction(PulsarFunctionOperations<?> function, PulsarAdmin admin) {
		return switch (function.stopPolicy()) {
			case NONE -> {
				this.logger.info(() -> "No stop policy for %s - leaving alone".formatted(functionDesc(function)));
				yield Optional.empty();
			}
			case STOP -> {
				this.logger.info(() -> "Stopping %s".formatted(functionDesc(function)));
				yield safeInvoke(() -> function.stop(admin));
			}
			case DELETE -> {
				this.logger.info(() -> "Deleting %s".formatted(functionDesc(function)));
				yield safeInvoke(() -> function.delete(admin));
			}
		};
	}

	private Optional<Exception> safeInvoke(Runnable invocation) {
		try {
			invocation.run();
		}
		catch (Exception ex) {
			return Optional.of(ex);
		}
		return Optional.empty();
	}

	private String functionDesc(PulsarFunctionOperations<?> function) {
		return "'%s' %s".formatted(function.name(), function.type().toString().toLowerCase(Locale.ROOT));
	}

	/**
	 * Indicates a failure of one or more function operations.
	 */
	public static class PulsarFunctionException extends PulsarException {

		private final Map<PulsarFunctionOperations<?>, Exception> failures;

		public PulsarFunctionException(String msg, Map<PulsarFunctionOperations<?>, Exception> failures) {
			super(msg);
			this.failures = failures;
		}

		public Map<PulsarFunctionOperations<?>, Exception> getFailures() {
			return this.failures;
		}

	}

}
