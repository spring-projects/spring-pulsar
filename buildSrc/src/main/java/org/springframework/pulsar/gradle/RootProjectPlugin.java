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

package org.springframework.pulsar.gradle;

import io.spring.gradle.convention.ArtifactoryPlugin;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.plugins.PluginManager;

import org.springframework.pulsar.gradle.publish.SpringNexusPublishPlugin;

/**
 * @author Chris Bono
 */
public class RootProjectPlugin implements Plugin<Project> {

	@Override
	public void apply(final Project project) {
		PluginManager pluginManager = project.getPluginManager();
		pluginManager.apply(BasePlugin.class);
		pluginManager.apply(SpringNexusPublishPlugin.class);
		pluginManager.apply(ArtifactoryPlugin.class);
		pluginManager.apply(SonarQubeConventionsPlugin.class);

		project.getRepositories().mavenCentral();

		Task finalizeDeployArtifacts = project.task("finalizeDeployArtifacts");
		if (ProjectUtils.isRelease(project) && project.hasProperty("ossrhUsername")) {
			finalizeDeployArtifacts.dependsOn(project.getTasks().findByName("closeAndReleaseOssrhStagingRepository"));
		}
	}

}
