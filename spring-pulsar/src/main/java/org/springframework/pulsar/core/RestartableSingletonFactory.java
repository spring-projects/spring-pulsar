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

import java.util.concurrent.atomic.AtomicBoolean;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.log.LogAccessor;
import org.springframework.util.Assert;

/**
 * Provides a simple base implementation for a restartable singleton factory.
 * <p>
 * It is restartable in the sense that it can be stopped and then started and still be in
 * a usable state.
 * <p>
 * Because it releases its resources when {@link SmartLifecycle#stop() stopped} and
 * re-acquires them when subsequently {@link SmartLifecycle#start() started}, it can also
 * be used as a base implementation for coordinated checkpoint and restore.
 *
 * @param <T> the bean type
 * @author Chris Bono
 */
abstract class RestartableSingletonFactory<T> extends RestartableComponentBase implements InitializingBean {

	private final LogAccessor logger = new LogAccessor(this.getClass());

	private final AtomicBoolean initialized = new AtomicBoolean(false);

	private @Nullable T instance;

	protected RestartableSingletonFactory() {
		super();
	}

	protected RestartableSingletonFactory(T instance) {
		super();
		Assert.notNull(instance, () -> "instance must not be null");
		this.instance = instance;
		this.initialized.set(true);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		ensureInstanceCreated();
	}

	@Override
	public void doStart() {
		ensureInstanceCreated();
	}

	private void ensureInstanceCreated() {
		if (this.initialized.compareAndSet(false, true)) {
			this.logger.debug(() -> "Creating instance");
			this.instance = createInstance();
		}
	}

	@Override
	public void doStop() {
		if (this.instance != null) {
			this.logger.debug(() -> "Stopping instance");
			stopInstance(this.instance);
			if (this.discardInstanceAfterStop()) {
				this.logger.debug(() -> "Discarding instance");
				this.instance = null;
			}
		}
		this.initialized.set(false);
	}

	@Override
	public void destroy() {
		super.destroy();
		this.instance = null;
	}

	/**
	 * Gets the singleton instance.
	 * @return the singleton instance
	 */
	@Nullable public final T getInstance() {
		return this.instance;
	}

	/**
	 * Gets the singleton instance.
	 * @return the non-null singleton instance
	 * @throws IllegalArgumentException if the instance is null
	 */
	public final T getRequiredInstance() {
		Assert.notNull(this.instance, "The instance must be set prior to calling this method");
		return this.instance;
	}

	/**
	 * Template method that subclasses must override to construct the backing singleton
	 * instance returned by this factory.
	 * <p>
	 * Implementations should throw a {@link RuntimeException} if an error occurs during
	 * creation.
	 * <p>
	 * Invoked on {@link #afterPropertiesSet() initialization} of this bean if the
	 * instance has not already been set via the constructor OR during {@link #start()} if
	 * the instance is null.
	 * @return the single object managed by the factory
	 */
	protected abstract T createInstance();

	/**
	 * Callback to allow the singleton instance to be &quot;stopped&quot; (ie. allow it to
	 * release any resources).
	 * <p>
	 * Implementations should throw a {@link RuntimeException} if an error occurs during
	 * destruction.
	 * <p>
	 * The default implementation is empty.
	 * @param instance the singleton instance, as returned by {@link #createInstance()}
	 */
	protected void stopInstance(T instance) {
	}

	/**
	 * Whether to discard the singleton instance (set reference to it null) when stopped
	 * (default is true).
	 * @return whether to discard the singleton when stopped
	 */
	protected boolean discardInstanceAfterStop() {
		return true;
	}

	/**
	 * Whether the singleton instance has been initialized.
	 * @return whether the singleton instance has been initialized
	 */
	protected boolean initialized() {
		return this.initialized.get();
	}

}
