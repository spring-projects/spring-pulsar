package org.springframework.pulsar.gradle.publish;

import java.util.stream.Collectors;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlatformPlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.publish.PublishingExtension;
import org.gradle.api.publish.maven.MavenArtifactSet;
import org.gradle.api.publish.maven.MavenPublication;
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin;

public class PublishAllJavaComponentsPlugin implements Plugin<Project> {

	@Override
	public void apply(Project project) {
		project.getPlugins().withType(MavenPublishPlugin.class).all((mavenPublish) -> {
			PublishingExtension publishing = project.getExtensions().getByType(PublishingExtension.class);
			publishing.getPublications().create("mavenJava", MavenPublication.class, maven -> {
				project.getPlugins().withType(JavaPlugin.class, (plugin) -> {
					maven.from(project.getComponents().getByName("java"));
					// Workaround issue where build/classes and build/resources are attempting to be signed
					maven.setArtifacts(maven.getArtifacts().stream().filter((ma) -> ma.getFile().isFile()).collect(Collectors.toSet()));
				});
				project.getPlugins().withType(JavaPlatformPlugin.class, (plugin) -> {
					maven.from(project.getComponents().getByName("javaPlatform"));
				});
			});
		});
	}

}
