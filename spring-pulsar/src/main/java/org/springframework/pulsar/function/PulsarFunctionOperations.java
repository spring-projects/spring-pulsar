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

import java.util.Optional;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.io.SourceConfig;

import org.springframework.core.log.LogAccessor;
import org.springframework.pulsar.PulsarException;

/**
 * Provides operations for a particular function type.
 *
 * <h3>Function Types</h3>
 *
 * <p>
 * The term 'function' is meant to cover Pulsar IO connectors (sources and sinks) as well
 * as Pulsar Functions (user defined functions).
 *
 * <h3>Motivation</h3>
 *
 * <p>
 * The underlying Pulsar function model treats source, sink, and function completely
 * separately, including its config objects and API calls. This operations abstraction
 * allows them to be treated as related during processing.
 *
 * @param <T> the function config (one of {@link SourceConfig}, {@link SinkConfig},
 * {@link FunctionConfig})
 * @author Chris Bono
 */
public interface PulsarFunctionOperations<T> {

	/**
	 * Logger used in default methods.
	 */
	LogAccessor logger = new LogAccessor(PulsarFunctionOperations.class);

	/**
	 * Gets the name of the function.
	 * @return the name of the function
	 */
	String name();

	/**
	 * Gets the type of function the operations handles.
	 * @return the type of the function
	 */
	FunctionType type();

	/**
	 * Gets the url or path to the archive that represents the function.
	 * @return the url or path to the archive that represents function
	 */
	String archive();

	/**
	 * Gets the action to take on the function when the server is stopped.
	 * @return the function stop policy
	 */
	FunctionStopPolicy stopPolicy();

	/**
	 * Determines if a function already exists.
	 * @param admin the admin client
	 * @return {@code true} if function already exists
	 * @throws PulsarAdminException if anything goes wrong
	 */
	default boolean functionExists(PulsarAdmin admin) throws PulsarAdminException {
		return getIfExists(admin).isPresent();
	}

	/**
	 * Gets the configuration details for a function if it exists.
	 * @param admin the admin client
	 * @return the optional current config of the function or empty if the function does
	 * not exist
	 * @throws PulsarAdminException if anything else goes wrong
	 */
	default Optional<T> getIfExists(PulsarAdmin admin) throws PulsarAdminException {
		try {
			return Optional.of(get(admin));
		}
		catch (NotFoundException ex) {
			logger.trace(ex, () -> "Function not found: " + name());
			return Optional.empty();
		}
	}

	/**
	 * Gets the configuration details for an existing function.
	 * @param admin the admin client
	 * @return the current config of the existing function
	 * @throws NotFoundException if function does not exist
	 * @throws PulsarAdminException if anything else goes wrong
	 */
	T get(PulsarAdmin admin) throws PulsarAdminException;

	/**
	 * Creates the function using the file-based create api.
	 * @param admin the admin client
	 * @throws PulsarAdminException if anything goes wrong
	 */
	void create(PulsarAdmin admin) throws PulsarAdminException;

	/**
	 * Creates the function using the url-based create api.
	 * @param admin the admin client
	 * @throws PulsarAdminException if anything goes wrong
	 */
	void createWithUrl(PulsarAdmin admin) throws PulsarAdminException;

	/**
	 * Updates the function using the file-based update api.
	 * @param admin the admin client
	 * @throws PulsarAdminException if anything goes wrong
	 */
	void update(PulsarAdmin admin) throws PulsarAdminException;

	/**
	 * Updates the function using the url-based update api.
	 * @param admin the admin client
	 * @throws PulsarAdminException if anything goes wrong
	 */
	void updateWithUrl(PulsarAdmin admin) throws PulsarAdminException;

	/**
	 * Stops the function.
	 * @param admin the admin client
	 * @throws PulsarException if anything goes wrong
	 */
	void stop(PulsarAdmin admin);

	/**
	 * Deletes the function.
	 * @param admin the admin client
	 * @throws PulsarException if anything goes wrong
	 */
	void delete(PulsarAdmin admin);

	/**
	 * The type of function the operations handle.
	 */
	enum FunctionType {

		/** A user-defined Pulsar function. */
		FUNCTION,

		/** A Pulsar sink connector. */
		SINK,

		/** A Pulsar source connector. */
		SOURCE

	}

	/**
	 * The action to take on the function when the server stops.
	 */
	enum FunctionStopPolicy {

		/** Do nothing - leave the function alone. */
		NONE,

		/** Stop the function if running. */
		STOP,

		/** Delete the function. */
		DELETE

	}

}
