/*
 * Copyright 2019-2022 the original author or authors.
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

package org.springframework.pulsar.gradle.versions;

import org.gradle.api.Plugin;
import org.gradle.api.Project;

public class UpdateProjectVersionPlugin implements Plugin<Project> {
	@Override
	public void apply(Project project) {
		project.getTasks().register("updateToReleaseVersion", UpdateToReleaseVersionTask.class, updateToReleaseVersionTask -> {
			updateToReleaseVersionTask.setGroup("Release");
			updateToReleaseVersionTask.setDescription("""
   				Updates the project version to the release version in gradle.properties and
   				the boot version used by docs to the upcoming boot release version in libs.versions.toml.""");
			updateToReleaseVersionTask.setReleaseVersion((String) project.findProperty("releaseVersion"));
		});
		project.getTasks().register("updateToSnapshotVersion", UpdateToSnapshotVersionTask.class, updateToSnapshotVersionTask -> {
			updateToSnapshotVersionTask.setGroup("Release");
			updateToSnapshotVersionTask.setDescription("""
   				Updates the project version to the next snapshot version and the project version 
   				used by samples to the current released version in gradle.properties.""");
		});
		project.getTasks().register("updateToNextBootSnapshotVersion", UpdateToNextBootSnapshotVersionTask.class, updateToNextBootSnapshotVersionTask -> {
			updateToNextBootSnapshotVersionTask.setGroup("Release");
			updateToNextBootSnapshotVersionTask.setDescription("""
   				Updates the project version used by samples to the current project version in 
   				gradle.properties and boot version used by docs and samples to the next boot 
   				snapshot version in libs.versions.toml.""");
		});

	}
}
