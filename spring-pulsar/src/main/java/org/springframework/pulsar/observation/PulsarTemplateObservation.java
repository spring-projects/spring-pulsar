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

import org.springframework.pulsar.core.PulsarTemplate;

import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.Observation;
import io.micrometer.observation.Observation.Context;
import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.docs.ObservationDocumentation;

/**
 * An {@link Observation} for {@link PulsarTemplate}.
 *
 * @author Chris Bono
 */
public enum PulsarTemplateObservation implements ObservationDocumentation {

	/**
	 * Observation created when a Pulsar template sends a message.
	 */
	TEMPLATE_OBSERVATION {

		@Override
		public Class<? extends ObservationConvention<? extends Context>> getDefaultConvention() {
			return DefaultPulsarTemplateObservationConvention.class;
		}

		@Override
		public String getPrefix() {
			return "spring.pulsar.template";
		}

		@Override
		public KeyName[] getLowCardinalityKeyNames() {
			return TemplateLowCardinalityTags.values();
		}

	};

	/**
	 * Low cardinality tags.
	 */
	public enum TemplateLowCardinalityTags implements KeyName {

		/**
		 * Bean name of the template that sent the message.
		 */
		BEAN_NAME {

			@Override
			public String asString() {
				return "spring.pulsar.template.name";
			}

		}

	}

}
