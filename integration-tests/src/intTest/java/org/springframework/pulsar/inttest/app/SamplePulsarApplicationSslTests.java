/*
 * Copyright 2012-2023 the original author or authors.
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

package org.springframework.pulsar.inttest.app;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Import;

@ExtendWith(OutputCaptureExtension.class)
class SamplePulsarApplicationSslTests {

	@Nested
	@Import(SampleJksBasedSslConfig.class)
	class JksBasedSslTests implements PulsarContainerWithJksBasedSslTestSupport {

		@Nested
		class ImperativeAppTests implements SpringBootTestImperativeApp {

		}

		@Nested
		class ReactiveAppTests implements SpringBootTestReactiveApp {

		}

	}

	@Nested
	@Import(SamplePemBasedSslConfig.class)
	class PemBasedSslTests implements PulsarContainerWithPemBasedSslTestSupport {

		@Nested
		class ImperativeAppTests implements SpringBootTestImperativeApp {

		}

		@Nested
		class ReactiveAppTests implements SpringBootTestReactiveApp {

		}

	}

}
