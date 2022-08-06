package org.springframework.pulsar.gradle;

import org.gradle.api.Project;

public final class ProjectUtils {

	private ProjectUtils() {
	}

	public static boolean isSnapshot(Project project) {
		return projectVersion(project).endsWith("-SNAPSHOT");
	}

	public static boolean isMilestone(Project project) {
		String projectVersion = projectVersion(project);
		return projectVersion.matches("^.*[.-]M\\d+$") || projectVersion.matches("^.*[.-]RC\\d+$");
	}

	public static boolean isRelease(Project project) {
		return !(isSnapshot(project) || isMilestone(project));
	}

	private static String projectVersion(Project project) {
		return String.valueOf(project.getVersion());
	}
}
