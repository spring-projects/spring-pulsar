/*
 * Copyright 2022-2022 the original author or authors.
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

package org.springframework.pulsar.gradle.classpath;

import java.util.Set;

import org.gradle.api.Task;
import org.gradle.api.artifacts.ModuleVersionIdentifier;

import org.springframework.boot.gradle.classpath.CheckClasspathForProhibitedDependencies;

/**
 * Extends the Spring Boot {@link Task} for checking the classpath for prohibited dependencies in a more lenient fashion
 * and allows the PulsarClient to bring in some of the {@code javax.*} dependencies.
 *
 * @author Chris Bono
 */
public class LenientCheckClasspathForProhibitedDependencies extends CheckClasspathForProhibitedDependencies {

	private static Set<String> OVERRIDE_PROHIBITED_DEPENDENCIES = Set.of(
			"javax.validation:validation-api",
			"javax.ws.rs:javax.ws.rs-api",
			"javax.inject:javax.inject",
			"javax.xml.bind:jaxb-api",
			"commons-logging:commons-logging");

	@Override
	protected boolean overrideProhibited(ModuleVersionIdentifier id) {
		return OVERRIDE_PROHIBITED_DEPENDENCIES.contains(id.getGroup() + ":" + id.getName());
	}
}
