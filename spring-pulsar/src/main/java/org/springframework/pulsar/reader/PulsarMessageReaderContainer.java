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

package org.springframework.pulsar.reader;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.pulsar.core.ReaderBuilderCustomizer;

/**
 * Internal abstraction used by the framework representing a message listener container.
 * Not meant to be implemented externally.
 *
 * @author Soby Chacko
 */
public sealed interface PulsarMessageReaderContainer
		extends SmartLifecycle, DisposableBean permits AbstractPulsarMessageReaderContainer {

	void setupReaderListener(Object messageListener);

	default PulsarReaderContainerProperties getContainerProperties() {
		throw new UnsupportedOperationException("This container doesn't support retrieving its properties");
	}

	@Override
	default void destroy() {
		stop();
	}

	default void setAutoStartup(boolean autoStartup) {
		// empty
	}

	/**
	 * Set a reader customizer on this container.
	 * @param readerBuilderCustomizer {@link ReaderBuilderCustomizer}
	 */
	void setReaderCustomizer(ReaderBuilderCustomizer<?> readerBuilderCustomizer);

}
