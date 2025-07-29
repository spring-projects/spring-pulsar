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

import java.time.Duration;

import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.MountableFile;

import org.springframework.pulsar.test.support.PulsarTestContainerSupport;

/**
 * A {@link PulsarContainer} with TLS (SSL) configuration.
 *
 * @author Chris Bono
 */
final class PulsarContainerWithSsl extends PulsarContainer {

	static final int BROKER_TLS_PORT = 6651;

	static final int BROKER_HTTP_TLS_PORT = 8081;

	static PulsarContainerWithSsl withJksBasedTls() {
		return new PulsarContainerWithSsl(false);
	}

	static PulsarContainerWithSsl withPemBasedTls() {
		return new PulsarContainerWithSsl(true);
	}

	private PulsarContainerWithSsl(boolean pemBasedTls) {
		super(PulsarTestContainerSupport.getPulsarImage());
		withStartupAttempts(2);
		withStartupTimeout(Duration.ofMinutes(3));
		withEnv("PF_ENV_DEBUG", "1");

		// TLS ports
		addExposedPorts(BROKER_TLS_PORT, BROKER_HTTP_TLS_PORT);
		withEnv("PULSAR_PREFIX_brokerServicePortTls", String.valueOf(BROKER_TLS_PORT));
		withEnv("PULSAR_PREFIX_webServicePortTls", String.valueOf(BROKER_HTTP_TLS_PORT));

		// Enable mTLS
		withEnv("PULSAR_PREFIX_tlsEnabled", "true");
		withEnv("PULSAR_PREFIX_tlsRequireTrustedClientCertOnConnect", "true");

		if (pemBasedTls) {
			// PEM based TLS
			withCopyFileToContainer(MountableFile.forClasspathResource("/ssl/pem/test-ca.crt"),
					"/pulsar/ssl/app/test-ca.crt");
			withCopyFileToContainer(MountableFile.forClasspathResource("/ssl/pem/test-server.crt"),
					"/pulsar/ssl/app/test-server.crt");
			withCopyFileToContainer(MountableFile.forClasspathResource("/ssl/pem/test-server.key"),
					"/pulsar/ssl/app/test-server.key");
			withCopyFileToContainer(MountableFile.forClasspathResource("/ssl/pem/test-client.crt"),
					"/pulsar/ssl/app/test-client.crt");
			withCopyFileToContainer(MountableFile.forClasspathResource("/ssl/pem/test-client.key"),
					"/pulsar/ssl/app/test-client.key");

			// Pulsar client config
			withEnv("PULSAR_PREFIX_tlsTrustCertsFilePath", "/pulsar/ssl/app/test-ca.crt");
			withEnv("PULSAR_PREFIX_tlsCertificateFilePath", "/pulsar/ssl/app/test-server.crt");
			withEnv("PULSAR_PREFIX_tlsKeyFilePath", "/pulsar/ssl/app/test-server.key");

			// Admin client config
			withEnv("PULSAR_PREFIX_brokerClientTlsEnabled", "true");
			withEnv("PULSAR_PREFIX_brokerClientTrustCertsFilePath", "/pulsar/ssl/app/test-ca.crt");
			withEnv("PULSAR_PREFIX_brokerClientCertificateFilePath", "/pulsar/ssl/app/test-client.crt");
			withEnv("PULSAR_PREFIX_brokerClientKeyFilePath", "/pulsar/ssl/app/test-client.key");
		}
		else {
			// JKS based TLS
			withCopyFileToContainer(MountableFile.forClasspathResource("/ssl/jks/test-ca.p12"),
					"/pulsar/ssl/app/test-ca.p12");
			withCopyFileToContainer(MountableFile.forClasspathResource("/ssl/jks/test-server.p12"),
					"/pulsar/ssl/app/test-server.p12");
			withCopyFileToContainer(MountableFile.forClasspathResource("/ssl/jks/test-client.p12"),
					"/pulsar/ssl/app/test-client.p12");

			// Enable key store TLS
			withEnv("PULSAR_PREFIX_tlsEnabledWithKeyStore", "true");

			// Pulsar client config
			withEnv("PULSAR_PREFIX_tlsEnabledWithKeyStore", "true");
			withEnv("PULSAR_PREFIX_tlsKeyStoreType", "PKCS12");
			withEnv("PULSAR_PREFIX_tlsKeyStore", "/pulsar/ssl/app/test-server.p12");
			withEnv("PULSAR_PREFIX_tlsKeyStorePassword", "password");
			withEnv("PULSAR_PREFIX_tlsTrustStoreType", "PKCS12");
			withEnv("PULSAR_PREFIX_tlsTrustStore", "/pulsar/ssl/app/test-ca.p12");
			withEnv("PULSAR_PREFIX_tlsTrustStorePassword", "password");

			// Admin client config
			withEnv("PULSAR_PREFIX_brokerClientTlsEnabled", "true");
			withEnv("PULSAR_PREFIX_brokerClientTlsEnabledWithKeyStore", "true");
			withEnv("PULSAR_PREFIX_brokerClientTlsKeyStoreType", "PKCS12");
			withEnv("PULSAR_PREFIX_brokerClientTlsKeyStore", "/pulsar/ssl/app/test-client.p12");
			withEnv("PULSAR_PREFIX_brokerClientTlsKeyStorePassword", "password");
			withEnv("PULSAR_PREFIX_brokerClientTlsTrustStoreType", "PKCS12");
			withEnv("PULSAR_PREFIX_brokerClientTlsTrustStore", "/pulsar/ssl/app/test-ca.p12");
			withEnv("PULSAR_PREFIX_brokerClientTlsTrustStorePassword", "password");
		}

	}

	String getPulsarBrokerTlsUrl() {
		return String.format("pulsar+ssl://%s:%s", getHost(), getMappedPort(BROKER_TLS_PORT));
	}

	String getHttpServiceTlsUrl() {
		return String.format("https://%s:%s", getHost(), getMappedPort(BROKER_HTTP_TLS_PORT));
	}

}
