package org.springframework.pulsar.gradle.publish;

import java.io.File;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.publish.PublishingExtension;
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin;

public class PublishLocalPlugin implements Plugin<Project> {

	@Override
	public void apply(Project project) {
		project.getPlugins().withType(MavenPublishPlugin.class).all(mavenPublish -> {
			PublishingExtension publishing = project.getExtensions().getByType(PublishingExtension.class);
			publishing.getRepositories().maven(maven -> {
				maven.setName("local");
				maven.setUrl(new File(project.getRootProject().getBuildDir(), "publications/repos"));
			});
		});
	}

}
