package org.springframework.pulsar.gradle

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPlatformPlugin

import org.springframework.pulsar.gradle.publish.PublishAllJavaComponentsPlugin
import org.springframework.pulsar.gradle.publish.SpringMavenPlugin

/**
 * @author Chris Bono
 */
public class MavenBomPlugin implements Plugin<Project> {

	public void apply(Project project) {
		project.plugins.apply(JavaPlatformPlugin)
		project.plugins.apply(SpringMavenPlugin)
		project.plugins.apply(PublishAllJavaComponentsPlugin)
	}
}
