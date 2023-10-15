/*
 * Copyright 2019-2022 the original author or authors.
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
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gradle.api.DefaultTask;
import org.gradle.api.Project;

import org.springframework.lang.Nullable;

public abstract class UpdateProjectVersionTask extends DefaultTask {

	static final String VERSION_PROPERTY = "version";

	static final String SPRING_BOOT_VERSION_PROPERTY = "springBootVersionForDocs";

	static final Pattern VERSION_PATTERN = Pattern.compile("^([0-9]+)\\.([0-9]+)\\.([0-9]+)(-M\\d+|-RC\\d+|-SNAPSHOT)?$");

	protected void updateVersionInGradleProperties(String newVersion) {
		this.updatePropertyInGradleProperties(VERSION_PROPERTY, (p) -> p.getVersion().toString(), newVersion);
	}

	protected void updatePropertyInGradleProperties(String propertyName, String newPropertyValue) {
		this.updatePropertyInGradleProperties(propertyName,
				(p) -> Objects.toString(p.findProperty(propertyName), ""), newPropertyValue);
	}

	protected void updatePropertyInGradleProperties(
			String propertyName,
			Function<Project, String> currentPropertyValueFromProject,
			String newPropertyValue) {
		String currentPropertyValue = currentPropertyValueFromProject.apply(getProject());
		File gradlePropertiesFile = getProject().getRootProject().file(Project.GRADLE_PROPERTIES);
		if (!gradlePropertiesFile.exists()) {
			throw new RuntimeException("No gradle.properties to update property in");
		}
		System.out.printf("Updating the %s property in %s from %s to %s%n",
				propertyName, Project.GRADLE_PROPERTIES, currentPropertyValue, newPropertyValue);
		FileUtils.replaceFileText(gradlePropertiesFile, (gradlePropertiesText) -> {
			gradlePropertiesText = gradlePropertiesText.replace(
					"%s=%s".formatted(propertyName, currentPropertyValue),
					"%s=%s".formatted(propertyName, newPropertyValue));
			return gradlePropertiesText;
		});
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

	record VersionInfo(String major, String minor, String patch, @Nullable String modifier) {
	}
}
