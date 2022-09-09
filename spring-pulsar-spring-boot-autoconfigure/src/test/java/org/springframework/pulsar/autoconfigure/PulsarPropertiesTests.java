/*
 * Copyright 2022 the original author or authors.
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

package org.springframework.pulsar.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link PulsarProperties}.
 *
 * @author Alexander Preuß
 */
public class PulsarPropertiesTests {

	@Test
	void encodedParamStringConversion() {
		Map<String, String> authParamsMap = Map.of("issuerUrl", "https://auth.server.cloud", "privateKey",
				"file://Users/xyz/key.json", "audience", "urn:sn:pulsar:abc:xyz");

		String encodedAuthParamString = PulsarProperties.convertToEncodedParamString(authParamsMap);
		assertThat(encodedAuthParamString).isEqualTo("\"{\"audience\":\"urn:sn:pulsar:abc:xyz\","
				+ "\"privateKey\":\"file://Users/xyz/key.json\",\"issuerUrl\":\"https://auth.server.cloud\"}\"");
	}

}
