package org.springframework.pulsar.gradle.publish;

import org.gradle.api.Plugin;
import org.gradle.api.Project;

import org.springframework.pulsar.gradle.ProjectUtils;

public class PublishArtifactsPlugin implements Plugin<Project> {

	@Override
	public void apply(Project project) {
		project.getTasks().register("publishArtifacts", publishArtifacts -> {
			publishArtifacts.setGroup("Publishing");
			publishArtifacts.setDescription("Publish the artifacts to either Artifactory or Maven Central based on the version");
			if (ProjectUtils.isRelease(project)) {
				publishArtifacts.dependsOn("publishToOssrh");
			}
			else {
				publishArtifacts.dependsOn("artifactoryPublish");
			}
		});
	}

}
