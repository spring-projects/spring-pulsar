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

package org.springframework.pulsar.observation;

import io.micrometer.common.KeyValues;

/**
 * Default {@link PulsarListenerObservationConvention} for Pulsar listener key values.
 *
 * @author Chris Bono
 */
public class DefaultPulsarListenerObservationConvention implements PulsarListenerObservationConvention {

	/**
	 * A singleton instance of the convention.
	 */
	public static final DefaultPulsarListenerObservationConvention INSTANCE = new DefaultPulsarListenerObservationConvention();

	@Override
	public KeyValues getLowCardinalityKeyValues(PulsarMessageReceiverContext context) {
		return KeyValues.of(PulsarListenerObservation.ListenerLowCardinalityTags.LISTENER_ID.asString(),
				context.getListenerId());
	}

	// Remove once addressed:
	// https://github.com/micrometer-metrics/micrometer-docs-generator/issues/30
	@Override
	public String getName() {
		return "spring.pulsar.listener";
	}

	@Override
	public String getContextualName(PulsarMessageReceiverContext context) {
		return context.getSource() + " receive";
	}

}
