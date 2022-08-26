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

import java.io.File;
import java.io.IOException;

import org.gradle.api.DefaultTask;
import org.gradle.api.Task;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

/**
 * {@link Task} used to document auto-configuration classes.
 *
 * @author Andy Wilkinson
 * @author Phillip Webb
 * @author Chris Bono
 * @author Alexander Preuß
 */
public class DocumentConfigurationProperties extends DefaultTask {

	private FileCollection configurationPropertyMetadata;

	private File outputDir;

	@InputFiles
	@PathSensitive(PathSensitivity.RELATIVE)
	public FileCollection getConfigurationPropertyMetadata() {
		return this.configurationPropertyMetadata;
	}

	public void setConfigurationPropertyMetadata(FileCollection configurationPropertyMetadata) {
		this.configurationPropertyMetadata = configurationPropertyMetadata;
	}

	@OutputDirectory
	public File getOutputDir() {
		return this.outputDir;
	}

	public void setOutputDir(File outputDir) {
		this.outputDir = outputDir;
	}

	@TaskAction
	void documentConfigurationProperties() throws IOException {
		Snippets snippets = new Snippets(this.configurationPropertyMetadata);
		snippets.add("application-properties.pulsar-client", "Pulsar Client Properties", (c) -> c.accept("spring.pulsar.client"));
		snippets.add("application-properties.pulsar-producer", "Pulsar Producer Properties", (c) -> c.accept("spring.pulsar.producer"));
		snippets.add("application-properties.pulsar-consumer", "Pulsar Consumer Properties", (c) -> {
			c.accept("spring.pulsar.consumer");
			c.accept("spring.pulsar.listener");
		});
		snippets.add("application-properties.pulsar-administration", "Pulsar Administration Properties", (c) -> c.accept("spring.pulsar.administration"));
		snippets.writeTo(this.outputDir.toPath());
	}
}
