/*
 * Copyright 2012-2021 the original author or authors.
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

package org.springframework.pulsar.build;

import org.asciidoctor.gradle.jvm.AsciidoctorJPlugin;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

import org.springframework.pulsar.build.docs.asciidoc.AsciidoctorConventions;

/**
 * Plugin to apply conventions to projects that are part of Spring Pulsar's build.
 * Conventions are applied in response to various plugins being applied.
 *
 * When the {@link AsciidoctorJPlugin} is applied, the conventions in
 * {@link AsciidoctorConventions} are applied.
 *
 * @author Chris Bono
 */
public class ConventionsPlugin implements Plugin<Project> {

	@Override
	public void apply(Project project) {
		new AsciidoctorConventions().apply(project);
	}

}
