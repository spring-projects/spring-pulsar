package org.springframework.pulsar.gradle.check;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.testing.Test;
import org.gradle.testing.jacoco.plugins.JacocoPlugin;
import org.gradle.testing.jacoco.plugins.JacocoPluginExtension;
import org.gradle.testing.jacoco.tasks.JacocoReport;

/**
 * Adds a version of jacoco to use and makes check depend on jacocoTestReport.
 *
 * @author Chris Bono
 */
public class JacocoConventionsPlugin implements Plugin<Project> {

	@Override
	public void apply(final Project project) {
		project.getPlugins().withType(JavaPlugin.class, (javaPlugin) -> {
			project.getPluginManager().apply(JacocoPlugin.class);
			project.getExtensions().configure(JacocoPluginExtension.class,
					(jacocoExtension) -> jacocoExtension.setToolVersion("0.8.9"));
			project.getTasks().withType(Test.class, (test) ->
					project.getTasks().withType(JacocoReport.class, test::finalizedBy));
		});
	}
}
