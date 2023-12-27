/*
 * Copyright 2019-2023 the original author or authors.
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

import org.gradle.api.tasks.TaskAction;

public abstract class UpdateToSnapshotVersionTask extends UpdateProjectVersionTask {

	@TaskAction
	public void updateToSnapshotVersion() {
		String currentVersion = getProject().getVersion().toString();
		updateVersionInGradleProperties(calculateNextSnapshotVersion(currentVersion));
		updateVersionInTomlVersions(SPRING_BOOT_VERSION_PROPERTY, this::calculateNextSnapshotVersion);
	}

	private String calculateNextSnapshotVersion(String version) {
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
}
