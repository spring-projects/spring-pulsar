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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.functions.Utils;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.PulsarException;
import org.springframework.pulsar.core.PulsarAdministration;

/**
 * Responsible for creating and updating any user-defined Pulsar functions, sinks, or
 * sources.
 *
 * @author Chris Bono
 */
public class PulsarFunctionAdministration implements SmartInitializingSingleton {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private final PulsarAdministration pulsarAdministration;

	private final ObjectProvider<PulsarFunction> pulsarFunctions;

	private final ObjectProvider<PulsarSink> pulsarSinks;

	private final ObjectProvider<PulsarSource> pulsarSources;

	private final boolean failFast;

	private final boolean propagateFailures;

	/**
	 * Construct a {@code PulsarFunctionAdministration} instance.
	 * @param pulsarAdministration the pulsar admin to make the API calls with
	 * @param pulsarFunctions provider of functions to create/update
	 * @param pulsarSinks provider of sinks to create/update
	 * @param pulsarSources provider of sources to create/update
	 * @param failFast whether to stop processing when a failure occurs
	 * @param propagateFailures whether to throw an exception when a failure occurs
	 */
	public PulsarFunctionAdministration(PulsarAdministration pulsarAdministration,
			ObjectProvider<PulsarFunction> pulsarFunctions, ObjectProvider<PulsarSink> pulsarSinks,
			ObjectProvider<PulsarSource> pulsarSources, boolean failFast, boolean propagateFailures) {
		this.pulsarAdministration = pulsarAdministration;
		this.pulsarFunctions = pulsarFunctions;
		this.pulsarSinks = pulsarSinks;
		this.pulsarSources = pulsarSources;
		this.failFast = failFast;
		this.propagateFailures = propagateFailures;
	}

	@Override
	public void afterSingletonsInstantiated() {
		createOrUpdateUserDefinedFunctions();
	}

	/**
	 * Creates or updates any Pulsar functions registered by the application.
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
		try (PulsarAdmin admin = this.pulsarAdministration.createAdminClient()) {
			// Concat the functions/sinks/sources into a single stream
			Stream<PulsarFunctionOperations<?>> allFunctions = Stream.concat(
					Stream.concat(this.pulsarFunctions.orderedStream(), this.pulsarSinks.orderedStream()),
					this.pulsarSources.orderedStream());

			// Spin through the combined stream and process each function
			Map<PulsarFunctionOperations<?>, Exception> failures = new LinkedHashMap<>();
			for (PulsarFunctionOperations<?> function : allFunctions.toList()) {
				Optional<Exception> failure = createOrUpdateFunction(function, admin);
				if (failure.isPresent()) {
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
		return String.format("%s %s (using %s archive: %s)", isUpdate ? "Updating" : "Creating", functionDesc(function),
				isUrlArchive ? "url" : "local", function.archive());
	}

	private String functionDesc(PulsarFunctionOperations<?> function) {
		return String.format("'%s' %s", function.name(), function.type().toString().toLowerCase());
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
