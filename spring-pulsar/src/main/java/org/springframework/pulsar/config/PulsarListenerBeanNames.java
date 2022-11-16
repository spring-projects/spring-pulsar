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

package org.springframework.pulsar.config;

/**
 * Constants for bean names that need to be accessed in multiple places by name.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public abstract class PulsarListenerBeanNames {

	/**
	 * The bean name of the internally managed Pulsar listener annotation processor.
	 */
	public static final String PULSAR_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME = "org.springframework.pulsar.config.internalPulsarListenerAnnotationProcessor";

	/**
	 * The bean name of the internally managed Pulsar listener endpoint registry.
	 */
	public static final String PULSAR_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME = "org.springframework.pulsar.config.internalPulsarListenerEndpointRegistry";

	/**
	 * The bean name of the internally managed Pulsar listener annotation processor.
	 */
	public static final String REACTIVE_PULSAR_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME = "org.springframework.pulsar.config.internalReactivePulsarListenerAnnotationProcessor";

	/**
	 * The bean name of the internally managed Pulsar listener endpoint registry.
	 */
	public static final String REACTIVE_PULSAR_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME = "org.springframework.pulsar.config.internalReactivePulsarListenerEndpointRegistry";

}
