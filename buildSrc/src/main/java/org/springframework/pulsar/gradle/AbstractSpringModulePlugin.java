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

package org.springframework.pulsar.gradle;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaLibraryPlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.PluginManager;

import org.springframework.pulsar.gradle.optional.OptionalDependenciesPlugin;

import io.spring.gradle.convention.RepositoryConventionPlugin;

/**
 * @author Chris Bono
 */
public abstract class AbstractSpringModulePlugin implements Plugin<Project> {

	@Override
	public void apply(final Project project) {
		PluginManager pluginManager = project.getPluginManager();
		pluginManager.apply(JavaPlugin.class);
		pluginManager.apply(JavaLibraryPlugin.class);
		pluginManager.apply(RepositoryConventionPlugin.class);
		pluginManager.apply(JavaConventionsPlugin.class);
		pluginManager.apply(OptionalDependenciesPlugin.class);
		additionalPlugins(project);
	}

	protected abstract void additionalPlugins(Project project);

}
