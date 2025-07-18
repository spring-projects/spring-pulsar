/*
 * Copyright 2019-2024 the original author or authors.
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

package org.springframework.pulsar.gradle.versions;

import java.io.File;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.artifacts.VersionCatalogsExtension;
import org.jspecify.annotations.Nullable;

public abstract class UpdateProjectVersionTask extends DefaultTask {

	static final String VERSION_PROPERTY = "version";

	static final String VERSION_FOR_SAMPLES_PROPERTY = "version.samples";

	static final String SPRING_BOOT_VERSION_FOR_SAMPLES_PROPERTY = "spring-boot";

	static final String SPRING_BOOT_VERSION_FOR_DOCS_PROPERTY = "spring-boot-for-docs";

	static final Pattern VERSION_PATTERN = Pattern.compile("^([0-9]+)\\.([0-9]+)\\.([0-9]+)(-M\\d+|-RC\\d+|-SNAPSHOT)?$");

	protected void updateVersionInGradleProperties(String versionPropertyName, String newVersion) {
		this.updatePropertyInFile(Project.GRADLE_PROPERTIES, versionPropertyName,
				(p) -> p.property(versionPropertyName).toString(),
				(currentValue) -> newVersion,
				(currentValue) -> "%s=%s".formatted(versionPropertyName, currentValue),
				(newValue) -> "%s=%s".formatted(versionPropertyName, newValue));
	}

	protected void updateVersionInTomlVersions(String versionPropertyName,
			Function<String, String> newPropertyValueGivenCurrentValue) {
		this.updatePropertyInFile("gradle/libs.versions.toml", versionPropertyName,
				(p) -> currentVersionInCatalog(p, versionPropertyName),
				newPropertyValueGivenCurrentValue,
				(currentValue) -> "%s = \"%s\"".formatted(versionPropertyName, currentValue),
				(newValue) -> "%s = \"%s\"".formatted(versionPropertyName, newValue));
	}

	protected void updateVersionInTomlVersions(String versionPropertyName, String sourcePropertyName,
			Function<String, String> newPropertyValueGivenCurrentValue) {
		var currentVersionPropertyValue = currentVersionInCatalog(getProject(), versionPropertyName);
		var currentSourcePropertyValue = currentVersionInCatalog(getProject(), sourcePropertyName);
		this.updatePropertyInFile("gradle/libs.versions.toml", versionPropertyName,
				(__) -> currentSourcePropertyValue,
				newPropertyValueGivenCurrentValue,
				(__) -> "%s = \"%s\"".formatted(versionPropertyName, currentVersionPropertyValue),
				(newValue) -> "%s = \"%s\"".formatted(versionPropertyName, newValue));
	}

	protected void updatePropertyInFile(String propertyFile, String propertyName,
			Function<Project, String> currentPropertyValueGivenProject,
			Function<String, String> newPropertyValueGivenCurrentValue,
			Function<String, String> expectedCurrentPropertyEntryInFile,
			Function<String, String> newPropertyEntryInFile) {
		File file = getProject().getRootProject().file(propertyFile);
		if (!file.exists()) {
			throw new RuntimeException("File not found at " + propertyFile);
		}
		String currentValue = currentPropertyValueGivenProject.apply(getProject());
		String newValue = newPropertyValueGivenCurrentValue.apply(currentValue);
		System.out.printf("Updating the %s property in %s from %s to %s%n", propertyName,
				propertyFile, currentValue, newValue);
		FileUtils.replaceFileText(file, (propertiesText) -> propertiesText.replace(
				expectedCurrentPropertyEntryInFile.apply(currentValue),
				newPropertyEntryInFile.apply(newValue)));
	}

	protected VersionInfo parseVersion(String version) {
		Matcher versionMatch = VERSION_PATTERN.matcher(version);
		if (versionMatch.find()) {
			String majorSegment = versionMatch.group(1);
			String minorSegment = versionMatch.group(2);
			String patchSegment = versionMatch.group(3);
			String modifier = versionMatch.group(4);
			return new VersionInfo(majorSegment, minorSegment, patchSegment, modifier);
		}
		else {
			throw new IllegalStateException(
					"Cannot extract version segment from %s as it does not conform to the expected format".formatted(version));
		}
	}

	protected String currentVersionInCatalog(Project project, String versionProperty) {
		VersionCatalogsExtension catalog = project.getExtensions().getByType(VersionCatalogsExtension.class);
		return catalog.named("libs").findVersion(versionProperty)
				.orElseThrow(() -> new IllegalStateException("% property not found".formatted(versionProperty)))
				.getDisplayName();
	}

	protected String calculateNextSnapshotVersion(String version) {
		VersionInfo versionSegments = parseVersion(version);
		String majorSegment = versionSegments.major();
		String minorSegment = versionSegments.minor();
		String patchSegment = versionSegments.patch();
		String modifier = versionSegments.modifier();
		System.out.println("modifier = " + modifier);
		if (modifier == null) {
			patchSegment = String.valueOf(Integer.parseInt(patchSegment) + 1);
		}
		return "%s.%s.%s-SNAPSHOT".formatted(majorSegment, minorSegment, patchSegment);
	}

	record VersionInfo(String major, String minor, String patch, @Nullable String modifier) {
	}
}
