/*
 * Copyright 2012-present the original author or authors.
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

import java.io.FileNotFoundException;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.pulsar.core.PulsarAdminBuilderCustomizer;
import org.springframework.pulsar.core.PulsarClientBuilderCustomizer;
import org.springframework.util.ResourceUtils;

@TestConfiguration(proxyBeanMethods = false)
class SampleJksBasedSslConfig {

	@Bean
	PulsarClientBuilderCustomizer pulsarClientJksSslCustomizer() {
		return (clientBuilder) -> {
			clientBuilder.allowTlsInsecureConnection(false);
			clientBuilder.enableTlsHostnameVerification(false);
			clientBuilder.useKeyStoreTls(true);
			clientBuilder.tlsTrustStoreType("PKCS12");
			clientBuilder.tlsTrustStorePath(this.resolvePath("classpath:ssl/jks/test-ca.p12"));
			clientBuilder.tlsTrustStorePassword("password");
			clientBuilder.tlsKeyStoreType("PKCS12");
			clientBuilder.tlsKeyStorePath(this.resolvePath("classpath:ssl/jks/test-client.p12"));
			clientBuilder.tlsKeyStorePassword("password");
		};
	}

	@Bean
	PulsarAdminBuilderCustomizer adminClientJksSslCustomizer() {
		return (clientBuilder) -> {
			clientBuilder.allowTlsInsecureConnection(false);
			clientBuilder.enableTlsHostnameVerification(false);
			clientBuilder.useKeyStoreTls(true);
			clientBuilder.tlsTrustStoreType("PKCS12");
			clientBuilder.tlsTrustStorePath(this.resolvePath("classpath:ssl/jks/test-ca.p12"));
			clientBuilder.tlsTrustStorePassword("password");
			clientBuilder.tlsKeyStoreType("PKCS12");
			clientBuilder.tlsKeyStorePath(this.resolvePath("classpath:ssl/jks/test-client.p12"));
			clientBuilder.tlsKeyStorePassword("password");
		};
	}

	/**
	 * Resolves a location into an actual path. The Pulsar client builders TLS related
	 * methods all expect the locations passed in to be file paths. Adding this resolve
	 * allows us to use 'classpath:' locations.
	 * @param resourceLocation the location of the resource
	 * @return path to the resource
	 */
	private String resolvePath(String resourceLocation) {
		try {
			return ResourceUtils.getURL(resourceLocation).getPath();
		}
		catch (FileNotFoundException ex) {
			throw new RuntimeException(ex);
		}
	}

}
