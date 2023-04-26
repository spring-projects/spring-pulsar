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

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaLibraryPlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.PluginManager;
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin;

import org.springframework.boot.gradle.JavaConventionsPlugin;
import org.springframework.boot.gradle.optional.OptionalDependenciesPlugin;
import org.springframework.pulsar.gradle.docs.asciidoc.AsciidoctorConventionsPlugin;
import org.springframework.pulsar.gradle.publish.MavenPublishingConventionsPlugin;
import org.springframework.pulsar.gradle.publish.PublishArtifactsPlugin;
import org.springframework.pulsar.gradle.publish.PublishLocalPlugin;
import org.springframework.pulsar.gradle.publish.SpringSigningPlugin;

import io.spring.gradle.convention.ArtifactoryPlugin;
import io.spring.gradle.convention.RepositoryConventionPlugin;

/**
 * @author Rob Winch
 * @author Chris Bono
 */
public class SpringDocsModulePlugin implements Plugin<Project> {

	@Override
	public void apply(final Project project) {
		PluginManager pluginManager = project.getPluginManager();
		pluginManager.apply(JavaPlugin.class);
		pluginManager.apply(RepositoryConventionPlugin.class);
		pluginManager.apply(JavaLibraryPlugin.class);
		pluginManager.apply(JavaConventionsPlugin.class);
		pluginManager.apply(AsciidoctorConventionsPlugin.class);
		pluginManager.apply(MavenPublishPlugin.class);
		pluginManager.apply(SpringSigningPlugin.class);
		pluginManager.apply(MavenPublishingConventionsPlugin.class);
		pluginManager.apply(PublishLocalPlugin.class);
		pluginManager.apply(PublishArtifactsPlugin.class);
		pluginManager.apply(ArtifactoryPlugin.class);
		pluginManager.apply(OptionalDependenciesPlugin.class);
	}
}
