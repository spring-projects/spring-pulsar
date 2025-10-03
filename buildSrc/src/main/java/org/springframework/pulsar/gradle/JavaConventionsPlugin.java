/*
 * Copyright 2012-present the original author or authors.
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

import java.util.Collections;
import java.util.Set;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ConfigurationContainer;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.DependencySet;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.quality.Checkstyle;
import org.gradle.api.plugins.quality.CheckstyleExtension;
import org.gradle.api.plugins.quality.CheckstylePlugin;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.api.tasks.javadoc.Javadoc;
import org.gradle.api.tasks.testing.Test;
import org.gradle.api.tasks.testing.logging.TestExceptionFormat;
import org.gradle.api.tasks.testing.logging.TestLogEvent;
import org.gradle.external.javadoc.CoreJavadocOptions;

import org.springframework.pulsar.gradle.optional.OptionalDependenciesPlugin;
import org.springframework.pulsar.gradle.testing.TestFailuresPlugin;

import io.spring.javaformat.gradle.SpringJavaFormatPlugin;
import io.spring.javaformat.gradle.tasks.CheckFormat;
import io.spring.javaformat.gradle.tasks.Format;

/**
 * Conventions that are applied in the presence of the {@link JavaBasePlugin}. When the
 * plugin is applied:
 *
 * <ul>
 * <li>The project is configured with source and target compatibility of 17
 * <li>{@link SpringJavaFormatPlugin Spring Java Format}, {@link CheckstylePlugin
 * Checkstyle} and {@link TestFailuresPlugin Test Failures}.
 * <li>{@link Test} tasks are configured:
 * <ul>
 * <li>to use JUnit Platform
 * <li>with a max heap of 1024M
 * <li>to run after any Checkstyle and format checking tasks
 * </ul>
 * <li>A {@code testRuntimeOnly} dependency upon
 * {@code org.junit.platform:junit-platform-launcher} is added to projects with the
 * {@link JavaPlugin} applied
 * <li>{@link JavaCompile}, {@link Javadoc}, and {@link Format} tasks are configured to
 * use UTF-8 encoding
 * <li>{@link JavaCompile} tasks are configured to:
 * <ul>
 * <li>Use {@code -parameters}.
 * <li>Treat warnings as errors
 * <li>Enable {@code unchecked}, {@code deprecation}, {@code rawtypes}, and {@code varags}
 * warnings
 * </ul>
 * <li>{@code spring-pulsar-dependencies} is used for dependency management</li>
 * </ul>
 *
 * <p/>
 *
 * @author Andy Wilkinson
 * @author Christoph Dreis
 * @author Mike Smithson
 * @author Scott Frederick
 * @author Chris Bono
 */
public class JavaConventionsPlugin implements Plugin<Project> {

	@Override
	public void apply(Project project) {
		project.getPlugins().withType(JavaBasePlugin.class, (java) -> {
			configureSpringJavaFormat(project);
			configureJavadocConventions(project);
			configureTestConventions(project);
			configureDependencyManagement(project);
		});
	}

	private void configureSpringJavaFormat(Project project) {
		project.getPlugins().apply(SpringJavaFormatPlugin.class);
		project.getTasks().withType(Format.class, (format) -> {
			format.setEncoding("UTF-8");
			project.getTasks().named("compileJava", JavaCompile.class, format::mustRunAfter);
		});
		project.getPlugins().apply(CheckstylePlugin.class);
		CheckstyleExtension checkstyle = project.getExtensions().getByType(CheckstyleExtension.class);
		checkstyle.setToolVersion("10.25.0");
		checkstyle.getConfigDirectory().set(project.getRootProject().file("src/checkstyle"));
		String version = SpringJavaFormatPlugin.class.getPackage().getImplementationVersion();
		DependencySet checkstyleDependencies = project.getConfigurations().getByName("checkstyle").getDependencies();
		checkstyleDependencies
				.add(project.getDependencies().create("com.puppycrawl.tools:checkstyle:" + checkstyle.getToolVersion()));
		checkstyleDependencies
				.add(project.getDependencies().create("io.spring.javaformat:spring-javaformat-checkstyle:" + version));
	}

	private void configureJavadocConventions(Project project) {
		project.getTasks().withType(Javadoc.class, (javadoc) -> {
			CoreJavadocOptions options = (CoreJavadocOptions) javadoc.getOptions();
			options.source("25");
			options.encoding("UTF-8");
			options.addStringOption("Xdoclint:none", "-quiet");
		});
	}

	private void configureTestConventions(Project project) {
		project.getPlugins().apply(TestFailuresPlugin.class);
		project.getTasks().withType(Test.class, (test) -> {
			test.useJUnitPlatform();
			test.setMaxHeapSize("1024M");
			test.testLogging(testLoggingContainer -> {
				testLoggingContainer.setEvents(Set.of(TestLogEvent.SKIPPED, TestLogEvent.FAILED));
				testLoggingContainer.setShowStandardStreams(project.hasProperty("showStandardStreams"));
				testLoggingContainer.setShowExceptions(true);
				testLoggingContainer.setShowStackTraces(true);
				testLoggingContainer.setExceptionFormat(TestExceptionFormat.FULL);
			});
			test.jvmArgs(
					"--add-opens", "java.base/java.lang=ALL-UNNAMED",
					"--add-opens", "java.base/java.util=ALL-UNNAMED",
					"--add-opens", "java.base/sun.net=ALL-UNNAMED"
					);
			test.getTestLogging().setShowStandardStreams(true);
			project.getTasks().withType(Checkstyle.class, test::mustRunAfter);
			project.getTasks().withType(CheckFormat.class, test::mustRunAfter);
		});
		project.getPlugins().withType(JavaPlugin.class, (javaPlugin) -> project.getDependencies()
				.add(JavaPlugin.TEST_RUNTIME_ONLY_CONFIGURATION_NAME, "org.junit.platform:junit-platform-launcher"));
	}

	private void configureDependencyManagement(Project project) {
		ConfigurationContainer configurations = project.getConfigurations();
		Configuration dependencyManagement = configurations.create("dependencyManagement", (configuration) -> {
			configuration.setVisible(false);
			configuration.setCanBeConsumed(false);
			configuration.setCanBeResolved(false);
		});
		configurations
				.matching((c) -> c.getName().endsWith("Classpath") || c.getName().toLowerCase().endsWith("annotationprocessor"))
				.all((c) -> c.extendsFrom(dependencyManagement));
		Dependency pulsarDependencies = project.getDependencies().enforcedPlatform(project.getDependencies()
				.project(Collections.singletonMap("path", ":spring-pulsar-dependencies")));
		dependencyManagement.getDependencies().add(pulsarDependencies);
		project.getPlugins().withType(OptionalDependenciesPlugin.class, (optionalDependencies) -> configurations
				.getByName(OptionalDependenciesPlugin.OPTIONAL_CONFIGURATION_NAME).extendsFrom(dependencyManagement));
	}

}
