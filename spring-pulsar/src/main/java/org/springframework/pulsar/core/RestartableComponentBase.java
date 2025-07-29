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

import java.util.concurrent.atomic.AtomicReference;

import org.springframework.core.log.LogAccessor;

/**
 * Provides a simple base implementation for a component that can be restarted (stopped
 * then started) and still be in a usable state.
 * <p>
 * Subclasses can use this as a base implementation for coordinated checkpoint and restore
 * by simply implementing the {@link #doStart() start} and/or {@link #doStop() stop}
 * callback to re-acquire and release resources, respectively.
 *
 * @author Chris Bono
 */
abstract class RestartableComponentBase implements RestartableComponentSupport {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private final AtomicReference<State> state = RestartableComponentSupport.initialState();

	@Override
	public AtomicReference<State> currentState() {
		return this.state;
	}

	@Override
	public LogAccessor logger() {
		return this.logger;
	}

}
