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

package org.springframework.pulsar.observation;

import io.micrometer.common.KeyValues;

/**
 * Default {@link PulsarTemplateObservationConvention} for Pulsar template key values.
 *
 * @author Chris Bono
 */
public class DefaultPulsarTemplateObservationConvention implements PulsarTemplateObservationConvention {

	/**
	 * A singleton instance of the convention.
	 */
	public static final DefaultPulsarTemplateObservationConvention INSTANCE = new DefaultPulsarTemplateObservationConvention();

	@Override
	public KeyValues getLowCardinalityKeyValues(PulsarMessageSenderContext context) {
		return KeyValues.of(PulsarTemplateObservation.TemplateLowCardinalityTags.BEAN_NAME.asString(),
				context.getBeanName());
	}

	// Remove once addressed:
	// https://github.com/micrometer-metrics/micrometer-docs-generator/issues/30
	@Override
	public String getName() {
		return "spring.pulsar.template";
	}

	@Override
	public String getContextualName(PulsarMessageSenderContext context) {
		return context.getDestination() + " send";
	}

}
