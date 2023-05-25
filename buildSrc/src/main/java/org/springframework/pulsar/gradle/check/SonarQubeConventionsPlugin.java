package org.springframework.pulsar.gradle.check;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.sonarqube.gradle.SonarQubeExtension;
import org.sonarqube.gradle.SonarQubePlugin;

import org.springframework.pulsar.gradle.ProjectLinks;

/**
 * Adds a version of SonarQube to use and configures it.
 *
 * @author Chris Bono
 */
public class SonarQubeConventionsPlugin implements Plugin<Project> {

	@Override
	public void apply(final Project project) {
		project.getPluginManager().apply(SonarQubePlugin.class);
		project.getExtensions().configure(SonarQubeExtension.class, (sonarExtension) ->
				sonarExtension.properties((sonarProperties) -> {
					sonarProperties.property("sonar.projectName", project.getName());
					sonarProperties.property("sonar.jacoco.reportPath", project.getBuildDir().getName() + "/jacoco.exec");
					sonarProperties.property("sonar.links.homepage", ProjectLinks.HOMEPAGE.link());
					sonarProperties.property("sonar.links.ci", ProjectLinks.CI.link());
					sonarProperties.property("sonar.links.issue", ProjectLinks.ISSUES.link());
					sonarProperties.property("sonar.links.scm", ProjectLinks.SCM_CONNECTION.link());
					sonarProperties.property("sonar.links.scm_dev", ProjectLinks.SCM_DEV_CONNECTION.link());
				}));
	}
}
