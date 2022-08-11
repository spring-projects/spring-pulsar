/*
 * Copyright 2012-2022 the original author or authors.
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

package org.springframework.pulsar.gradle.docs.configprops;

import java.util.stream.Collectors;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import org.springframework.util.StringUtils;

/**
 * {@link Plugin} for projects that define {@code @ConfigurationProperties}. When applied,
 * the plugin reacts to the presence of the {@link JavaPlugin} by:
 *
 * <ul>
 * <li>Adding a dependency on the configuration properties annotation processor.
 * <li>Configuring the additional metadata locations annotation processor compiler
 * argument.
 * <li>Adding the outputs of the processResources task as inputs of the compileJava task
 * to ensure that the additional metadata is available when the annotation processor runs.
 * <li>Registering a {@link CheckAdditionalSpringConfigurationMetadata} task and
 * configuring the {@code check} task to depend upon it.
 * <li>Defining an artifact for the resulting configuration property metadata so that it
 * can be consumed by downstream projects.
 * </ul>
 *
 * @author Andy Wilkinson
 * @author Chris Bono
 */
public class ConfigurationPropertiesPlugin implements Plugin<Project> {

	// TODO extend the one in boot and delete most of this

	/**
	 * Name of the {@link Configuration} that holds the configuration property metadata
	 * artifact.
	 */
	public static final String CONFIGURATION_PROPERTIES_METADATA_CONFIGURATION_NAME = "configurationPropertiesMetadata";

	/**
	 * Name of the {@link CheckAdditionalSpringConfigurationMetadata} task.
	 */
	public static final String CHECK_ADDITIONAL_SPRING_CONFIGURATION_METADATA_TASK_NAME = "checkAdditionalSpringConfigurationMetadata";

	@Override
	public void apply(Project project) {
		project.getPlugins().withType(JavaPlugin.class, (javaPlugin) -> {
			addConfigurationProcessorDependency(project);
			configureAdditionalMetadataLocationsCompilerArgument(project);
			registerCheckAdditionalMetadataTask(project);
			addMetadataArtifact(project);
		});
	}

	 private void addConfigurationProcessorDependency(Project project) {
		 Configuration annotationProcessors = project.getConfigurations()
				 .getByName(JavaPlugin.ANNOTATION_PROCESSOR_CONFIGURATION_NAME);
		 annotationProcessors.getDependencies().add(project.getDependencies().create(
				 "org.springframework.boot:spring-boot-configuration-processor"));
	 }

	private void addMetadataArtifact(Project project) {
		SourceSet mainSourceSet = project.getExtensions().getByType(JavaPluginExtension.class).getSourceSets()
				.getByName(SourceSet.MAIN_SOURCE_SET_NAME);
		project.getConfigurations().maybeCreate(CONFIGURATION_PROPERTIES_METADATA_CONFIGURATION_NAME);
		project.afterEvaluate((evaluatedProject) -> evaluatedProject.getArtifacts().add(
				CONFIGURATION_PROPERTIES_METADATA_CONFIGURATION_NAME,
				mainSourceSet.getJava().getDestinationDirectory().dir("META-INF/spring-configuration-metadata.json"),
				(artifact) -> artifact
						.builtBy(evaluatedProject.getTasks().getByName(mainSourceSet.getClassesTaskName()))));
	}

	private void configureAdditionalMetadataLocationsCompilerArgument(Project project) {
		JavaCompile compileJava = project.getTasks().withType(JavaCompile.class)
				.getByName(JavaPlugin.COMPILE_JAVA_TASK_NAME);
		((Task) compileJava).getInputs().files(project.getTasks().getByName(JavaPlugin.PROCESS_RESOURCES_TASK_NAME))
				.withPathSensitivity(PathSensitivity.RELATIVE).withPropertyName("processed resources");
		SourceSet mainSourceSet = project.getExtensions().getByType(JavaPluginExtension.class).getSourceSets()
				.getByName(SourceSet.MAIN_SOURCE_SET_NAME);
		compileJava.getOptions().getCompilerArgs()
				.add("-Aorg.springframework.boot.configurationprocessor.additionalMetadataLocations=" + StringUtils
						.collectionToCommaDelimitedString(mainSourceSet.getResources().getSourceDirectories().getFiles()
								.stream().map(project.getRootProject()::relativePath).collect(Collectors.toSet())));
	}

	private void registerCheckAdditionalMetadataTask(Project project) {
		TaskProvider<CheckAdditionalSpringConfigurationMetadata> checkConfigurationMetadata = project.getTasks()
				.register(CHECK_ADDITIONAL_SPRING_CONFIGURATION_METADATA_TASK_NAME,
						CheckAdditionalSpringConfigurationMetadata.class);
		checkConfigurationMetadata.configure((check) -> {
			SourceSet mainSourceSet = project.getExtensions().getByType(JavaPluginExtension.class).getSourceSets()
					.getByName(SourceSet.MAIN_SOURCE_SET_NAME);
			check.setSource(mainSourceSet.getResources());
			check.include("META-INF/additional-spring-configuration-metadata.json");
			check.getReportLocation().set(project.getLayout().getBuildDirectory()
					.file("reports/additional-spring-configuration-metadata/check.txt"));
		});
		project.getTasks().named(LifecycleBasePlugin.CHECK_TASK_NAME)
				.configure((check) -> check.dependsOn(checkConfigurationMetadata));
	}

}
