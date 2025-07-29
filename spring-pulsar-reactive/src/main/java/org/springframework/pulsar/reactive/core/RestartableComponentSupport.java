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

package org.springframework.pulsar.reactive.core;

import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.log.LogAccessor;

/**
 * Provides a simple base implementation for a component that can be restarted (stopped
 * then started) and still be in a usable state.
 * <p>
 * This is an interface that provides default methods that rely on the current component
 * state which must be maintained by the implementing component.
 * <p>
 * This can serve as a base implementation for coordinated checkpoint and restore by
 * simply implementing the {@link #doStart() start} and/or {@link #doStop() stop} callback
 * to re-acquire and release resources, respectively.
 * <p>
 * Implementors are required to provide the component state and a logger.
 *
 * @author Chris Bono
 */
interface RestartableComponentSupport extends SmartLifecycle, DisposableBean {

	/**
	 * Gets the initial state for the implementing component.
	 * @return the initial component state
	 */
	static AtomicReference<State> initialState() {
		return new AtomicReference<>(State.CREATED);
	}

	/**
	 * Callback to get the current state from the component.
	 * @return the current state of the component
	 */
	AtomicReference<State> currentState();

	/**
	 * Callback to get the component specific logger.
	 * @return the component specific logger
	 */
	LogAccessor logger();

	/**
	 * Lifecycle state of this factory.
	 */
	enum State {

		/** Component initially created. */
		CREATED,
		/** Component in the process of being started. */
		STARTING,
		/** Component has been started. */
		STARTED,
		/** Component in the process of being stopped. */
		STOPPING,
		/** Component has been stopped. */
		STOPPED,
		/** Component has been destroyed. */
		DESTROYED;

	}

	@Override
	default boolean isRunning() {
		return State.STARTED.equals(currentState().get());
	}

	@Override
	default void start() {
		State current = currentState().getAndUpdate(state -> isCreatedOrStopped(state) ? State.STARTING : state);
		if (isCreatedOrStopped(current)) {
			logger().debug(() -> "Starting...");
			doStart();
			currentState().set(State.STARTED);
			logger().debug(() -> "Started");
		}
	}

	private static boolean isCreatedOrStopped(@Nullable State state) {
		return State.CREATED.equals(state) || State.STOPPED.equals(state);
	}

	/**
	 * Callback invoked during startup - default implementation does nothing.
	 */
	default void doStart() {
	}

	@Override
	default void stop() {
		State current = currentState().getAndUpdate(state -> isCreatedOrStarted(state) ? State.STOPPING : state);
		if (isCreatedOrStarted(current)) {
			logger().debug(() -> "Stopping...");
			doStop();
			currentState().set(State.STOPPED);
			logger().debug(() -> "Stopped");
		}
	}

	private static boolean isCreatedOrStarted(@Nullable State state) {
		return State.CREATED.equals(state) || State.STARTED.equals(state);
	}

	/**
	 * Callback invoked during stop - default implementation does nothing.
	 */
	default void doStop() {
	}

	@Override
	default void destroy() {
		logger().debug(() -> "Destroying...");
		stop();
		currentState().set(State.DESTROYED);
		logger().debug(() -> "Destroyed");
	}

}
