package org.springframework.pulsar.gradle.publish;

import java.util.LinkedHashSet;
import java.util.stream.Collectors;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlatformPlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.publish.PublishingExtension;
import org.gradle.api.publish.maven.MavenPublication;
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin;

public class PublishAllJavaComponentsPlugin implements Plugin<Project> {

	@Override
	public void apply(Project project) {
		project.getPlugins().withType(MavenPublishPlugin.class).all((mavenPublish) -> {
			PublishingExtension publishing = project.getExtensions().getByType(PublishingExtension.class);

			// To get around some weirdness w/ the Signing plugin attempting to use java-classes-dir and java-resources-dir
			// as artifacts to sign, we load the artifacts into a hidden dummy MavenPublication using from(components.java)
			// and then filter out the unwanted artifacts. Finally, we add those to the actual mavenJava artifact.
			// NOTE: If MavenPublication.setArtifacts is called after MavenPublication.from then downstream issues will
			// occur when building due to artifacts modified exceptions.

			var filteredMainArtifacts = new LinkedHashSet<>();
			var hiddenMavenJavaPub = publishing.getPublications().create("hiddenMavenJava", MavenPublication.class, maven -> {
				project.getPlugins().withType(JavaPlugin.class, (plugin) -> {
					maven.from(project.getComponents().getByName("java"));
					filteredMainArtifacts.addAll(maven.getArtifacts().stream().filter((ma) -> ma.getFile().isFile()).collect(Collectors.toSet()));
				});
			});
			publishing.getPublications().remove(hiddenMavenJavaPub);
			publishing.getPublications().create("mavenJava", MavenPublication.class, maven -> {
				project.getPlugins().withType(JavaPlugin.class, (plugin) -> {
					filteredMainArtifacts.forEach((ma) -> maven.artifact(ma));
				});
				project.getPlugins().withType(JavaPlatformPlugin.class, (plugin) -> {
					maven.from(project.getComponents().getByName("javaPlatform"));
				});
			});
		});
	}

}
