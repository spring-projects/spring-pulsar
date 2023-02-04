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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gradle.api.tasks.TaskAction;

public abstract class UpdateToSnapshotVersionTask extends UpdateProjectVersionTask {

	private static final String RELEASE_VERSION_PATTERN = "^([0-9]+)\\.([0-9]+)\\.([0-9]+)(-M\\d+|-RC\\d+)?$";

	@TaskAction
	public void updateToSnapshotVersion() {
		String currentVersion = getProject().getVersion().toString();
		updateVersionInGradleProperties(calculateNextSnapshotVersion(currentVersion));
	}

	private String calculateNextSnapshotVersion(String currentVersion) {
		Pattern releaseVersionPattern = Pattern.compile(RELEASE_VERSION_PATTERN);
		Matcher releaseVersion = releaseVersionPattern.matcher(currentVersion);

		if (releaseVersion.find()) {
			String majorSegment = releaseVersion.group(1);
			String minorSegment = releaseVersion.group(2);
			String patchSegment = releaseVersion.group(3);
			String modifier = releaseVersion.group(4);
			if (modifier == null) {
				patchSegment = String.valueOf(Integer.parseInt(patchSegment) + 1);
			}
			System.out.println("modifier = " + modifier);
			return "%s.%s.%s-SNAPSHOT".formatted(majorSegment, minorSegment, patchSegment);
		}
		else {
			throw new IllegalStateException(
					"Cannot calculate next snapshot version because the current project version does not conform to the expected format");
		}
	}

}
