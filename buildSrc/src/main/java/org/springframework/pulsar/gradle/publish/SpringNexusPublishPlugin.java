package org.springframework.pulsar.gradle.publish;

import java.net.URI;
import java.time.Duration;

import io.github.gradlenexus.publishplugin.NexusPublishExtension;
import io.github.gradlenexus.publishplugin.NexusPublishPlugin;
import io.github.gradlenexus.publishplugin.NexusRepository;
import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

public class SpringNexusPublishPlugin implements Plugin<Project> {

	@Override
	public void apply(Project project) {
		project.getPlugins().apply(NexusPublishPlugin.class);
		NexusPublishExtension nexusPublishing = project.getExtensions().findByType(NexusPublishExtension.class);
		nexusPublishing.getRepositories().create("ossrh", nexusRepository -> {
			nexusRepository.getNexusUrl().set(URI.create("https://s01.oss.sonatype.org/service/local/"));
			nexusRepository.getSnapshotRepositoryUrl().set(
					URI.create("https://s01.oss.sonatype.org/content/repositories/snapshots/"));
		});
		nexusPublishing.getConnectTimeout().set(Duration.ofMinutes(3));
		nexusPublishing.getClientTimeout().set(Duration.ofMinutes(3));
	}

}
