/*
 * Copyright 2016-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.spring.gradle.convention

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.tasks.testing.Test
import org.gradle.plugins.ide.eclipse.EclipsePlugin
import org.gradle.plugins.ide.idea.IdeaPlugin

import org.springframework.boot.gradle.optional.OptionalDependenciesPlugin

/**
 * Adds support for integration tests to java projects.
 * <ul>
 * <li>Adds 'integrationTestCompile' and 'integrationTestRuntime' configurations</li>
 * <li>Adds 'src/integration-test/java' source test folder</li>
 * <li>Adds 'integrationTest' task to run integration tests</li>
 * </ul>
 *
 * @author Rob Winch
 * @author Chris Bono
 */
class IntegrationTestPlugin implements Plugin<Project> {

	@Override
	void apply(Project project) {
		project.plugins.withType(JavaPlugin.class) {
			applyJava(project)
		}
	}

	private applyJava(Project project) {
		if(!project.file('src/integration-test/').exists()) {
			// ensure we don't add if no tests
			return
		}
		project.configurations {
			integrationTestCompile {
				extendsFrom testImplementation
			}
			integrationTestRuntime {
				extendsFrom integrationTestCompile, testRuntimeClasspath, testRuntimeOnly
			}
			integrationTestCompileClasspath {
				extendsFrom integrationTestCompile
				canBeResolved = true
			}
			integrationTestRuntimeClasspath {
				extendsFrom integrationTestRuntime
				canBeResolved = true
			}
		}

		project.sourceSets {
			integrationTest {
				java.srcDir project.file('src/integration-test/java')
				resources.srcDir project.file('src/integration-test/resources')
				compileClasspath = project.sourceSets.main.output + project.sourceSets.test.output + project.configurations.integrationTestCompileClasspath
				runtimeClasspath = output + compileClasspath + project.configurations.integrationTestRuntimeClasspath
			}
		}

		Task integrationTestTask = project.tasks.create("integrationTest", Test) {
			description = 'Runs integration tests.'
			group = 'verification'

			testClassesDirs = project.sourceSets.integrationTest.output.classesDirs
			classpath = project.sourceSets.integrationTest.runtimeClasspath

			shouldRunAfter project.tasks.test

			useJUnitPlatform()
		}
		
		project.tasks.check.dependsOn integrationTestTask

		project.plugins.withType(OptionalDependenciesPlugin) {
			project.configurations {
				integrationTestCompile {
					extendsFrom optional
				}
			}
		}

		project.plugins.withType(IdeaPlugin) {
			project.idea {
				module {
					testSourceDirs += project.file('src/integration-test/java')
					scopes.TEST.plus += [ project.configurations.integrationTestCompileClasspath ]
				}
			}
		}

		project.plugins.withType(EclipsePlugin) {
			project.eclipse.classpath {
				plusConfigurations += [ project.configurations.integrationTestCompileClasspath ]
			}
		}
	}
}
