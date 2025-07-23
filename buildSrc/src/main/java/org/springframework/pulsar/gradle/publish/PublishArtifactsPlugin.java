package org.springframework.pulsar.gradle.publish;

import org.gradle.api.Plugin;
import org.gradle.api.Project;

import org.springframework.pulsar.gradle.ProjectUtils;

public class PublishArtifactsPlugin implements Plugin<Project> {

	@Override
	public void apply(Project project) {
		project.getTasks().register("stageForCentralPublish", centralPublish -> {
			centralPublish.setGroup("Publishing");
			centralPublish.setDescription("Stage the artifacts for Maven Central");
			centralPublish.dependsOn("publishAllPublicationsToLocalRepository");
		});
		project.getTasks().register("publishArtifacts", publishArtifacts -> {
			publishArtifacts.setGroup("Publishing");
			publishArtifacts.setDescription("Publish the artifacts to either Artifactory or Maven Central based on the version");
			if (ProjectUtils.isSnapshot(project) || project.getName().equals("spring-pulsar-docs")) {
				publishArtifacts.dependsOn("artifactoryPublish");
			}
			else {
				publishArtifacts.dependsOn("stageForCentralPublish");
			}
		});
	}

}
