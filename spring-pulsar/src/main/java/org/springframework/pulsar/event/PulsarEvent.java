/*
 * Copyright 2022 the original author or authors.
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

package org.springframework.pulsar.event;

import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;

/**
 * Base class for events.
 *
 * @author Soby Chacko
 */
public class PulsarEvent extends ApplicationEvent {

	private static final long serialVersionUID = 1L;

	private final Object container;

	public PulsarEvent(Object source, Object container) {
		super(source);
		this.container = container;
	}

	@SuppressWarnings("unchecked")
	public <T> T getContainer(Class<T> type) {
		Assert.isInstanceOf(type, this.container);
		return (T) this.container;
	}

	@SuppressWarnings("unchecked")
	public <T> T getSource(Class<T> type) {
		Assert.isInstanceOf(type, getSource());
		return (T) getSource();
	}

}
