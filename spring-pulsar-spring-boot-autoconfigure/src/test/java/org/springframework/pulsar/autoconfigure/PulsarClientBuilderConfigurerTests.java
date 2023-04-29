/*
 * Copyright 2023-2023 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.client.api.SizeUnit;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import org.springframework.pulsar.core.PulsarClientBuilderCustomizer;
import org.springframework.util.unit.DataSize;

/**
 * Tests for {@link PulsarClientBuilderConfigurer}.
 *
 * @author Chris Bono
 */
public class PulsarClientBuilderConfigurerTests {

	@Test
	void singleCustomizerIsApplied() {
		var customizer = mock(PulsarClientBuilderCustomizer.class);
		var configurer = new PulsarClientBuilderConfigurer(new PulsarProperties(), List.of(customizer));
		var clientBuilder = mock(ClientBuilder.class);
		configurer.configure(clientBuilder);
		verify(customizer).customize(clientBuilder);
	}

	@Test
	void multipleCustomizersAreAppliedInOrder() {
		var customizer1 = mock(PulsarClientBuilderCustomizer.class);
		var customizer2 = mock(PulsarClientBuilderCustomizer.class);
		var configurer = new PulsarClientBuilderConfigurer(new PulsarProperties(), List.of(customizer2, customizer1));
		var clientBuilder = mock(ClientBuilder.class);
		configurer.configure(clientBuilder);
		InOrder inOrder = inOrder(customizer1, customizer2);
		inOrder.verify(customizer2).customize(clientBuilder);
		inOrder.verify(customizer1).customize(clientBuilder);
	}

	@SuppressWarnings("deprecation")
	@Test
	void standardPropertiesAreApplied() {
		var pulsarProps = new PulsarProperties();
		var clientProps = pulsarProps.getClient();
		clientProps.setServiceUrl("my-service-url");
		clientProps.setListenerName("my-listener");
		clientProps.setOperationTimeout(Duration.ofSeconds(1));
		clientProps.setLookupTimeout(Duration.ofSeconds(2));
		clientProps.setNumIoThreads(3);
		clientProps.setNumListenerThreads(4);
		clientProps.setNumConnectionsPerBroker(5);
		clientProps.setUseTcpNoDelay(false);
		clientProps.setUseTls(true);
		clientProps.setTlsHostnameVerificationEnable(true);
		clientProps.setTlsTrustCertsFilePath("my-trust-certs-file-path");
		clientProps.setTlsCertificateFilePath("my-certificate-file-path");
		clientProps.setTlsKeyFilePath("my-key-file-path");
		clientProps.setTlsAllowInsecureConnection(true);
		clientProps.setUseKeyStoreTls(true);
		clientProps.setSslProvider("my-ssl-provider");
		clientProps.setTlsTrustStoreType("my-trust-store-type");
		clientProps.setTlsTrustStorePath("my-trust-store-path");
		clientProps.setTlsTrustStorePassword("my-trust-store-password");
		clientProps.setTlsCiphers(Set.of("my-tls-cipher"));
		clientProps.setTlsProtocols(Set.of("my-tls-protocol"));
		clientProps.setStatsInterval(Duration.ofSeconds(6));
		clientProps.setMaxConcurrentLookupRequest(7);
		clientProps.setMaxLookupRequest(8);
		clientProps.setMaxLookupRedirects(9);
		clientProps.setMaxNumberOfRejectedRequestPerConnection(10);
		clientProps.setKeepAliveInterval(Duration.ofSeconds(11));
		clientProps.setConnectionTimeout(Duration.ofSeconds(12));
		clientProps.setInitialBackoffInterval(Duration.ofSeconds(13));
		clientProps.setMaxBackoffInterval(Duration.ofSeconds(14));
		clientProps.setEnableBusyWait(true);
		clientProps.setMemoryLimit(DataSize.ofBytes(15));
		clientProps.setProxyServiceUrl("my-proxy-service-url");
		clientProps.setProxyProtocol(ProxyProtocol.SNI);
		clientProps.setEnableTransaction(true);
		clientProps.setDnsLookupBindAddress("my-dns-lookup-bind-address");
		clientProps.setDnsLookupBindPort(16);
		clientProps.setSocks5ProxyAddress("socks5://my-socks5-proxy-address:5150");
		clientProps.setSocks5ProxyUsername("my-socks5-proxy-username");
		clientProps.setSocks5ProxyPassword("my-socks5-proxy-password");

		var configurer = new PulsarClientBuilderConfigurer(pulsarProps, Collections.emptyList());
		var clientBuilder = mock(ClientBuilder.class);
		configurer.configure(clientBuilder);

		verify(clientBuilder).serviceUrl(clientProps.getServiceUrl());
		verify(clientBuilder).listenerName("my-listener");
		verify(clientBuilder).operationTimeout(1000, TimeUnit.MILLISECONDS);
		verify(clientBuilder).lookupTimeout(2000, TimeUnit.MILLISECONDS);
		verify(clientBuilder).ioThreads(3);
		verify(clientBuilder).listenerThreads(4);
		verify(clientBuilder).connectionsPerBroker(5);
		verify(clientBuilder).enableTcpNoDelay(false);
		verify(clientBuilder).enableTls(true);
		verify(clientBuilder).enableTlsHostnameVerification(true);
		verify(clientBuilder).tlsTrustCertsFilePath("my-trust-certs-file-path");
		verify(clientBuilder).tlsCertificateFilePath("my-certificate-file-path");
		verify(clientBuilder).tlsKeyFilePath("my-key-file-path");
		verify(clientBuilder).allowTlsInsecureConnection(true);
		verify(clientBuilder).useKeyStoreTls(true);
		verify(clientBuilder).sslProvider("my-ssl-provider");
		verify(clientBuilder).tlsTrustStoreType("my-trust-store-type");
		verify(clientBuilder).tlsTrustStorePath("my-trust-store-path");
		verify(clientBuilder).tlsTrustStorePassword("my-trust-store-password");
		verify(clientBuilder).tlsCiphers(Set.of("my-tls-cipher"));
		verify(clientBuilder).tlsProtocols(Set.of("my-tls-protocol"));
		verify(clientBuilder).statsInterval(6, TimeUnit.SECONDS);
		verify(clientBuilder).maxConcurrentLookupRequests(7);
		verify(clientBuilder).maxLookupRequests(8);
		verify(clientBuilder).maxLookupRedirects(9);
		verify(clientBuilder).maxNumberOfRejectedRequestPerConnection(10);
		verify(clientBuilder).keepAliveInterval(11000, TimeUnit.MILLISECONDS);
		verify(clientBuilder).connectionTimeout(12000, TimeUnit.MILLISECONDS);
		verify(clientBuilder).startingBackoffInterval(13000, TimeUnit.MILLISECONDS);
		verify(clientBuilder).maxBackoffInterval(14000, TimeUnit.MILLISECONDS);
		verify(clientBuilder).enableBusyWait(true);
		verify(clientBuilder).memoryLimit(15, SizeUnit.BYTES);
		verify(clientBuilder).proxyServiceUrl("my-proxy-service-url", ProxyProtocol.SNI);
		verify(clientBuilder).enableTransaction(true);
		verify(clientBuilder).dnsLookupBind("my-dns-lookup-bind-address", 16);
		verify(clientBuilder).socks5ProxyAddress(new InetSocketAddress("my-socks5-proxy-address", 5150));
		verify(clientBuilder).socks5ProxyUsername("my-socks5-proxy-username");
		verify(clientBuilder).socks5ProxyPassword("my-socks5-proxy-password");
	}

	@Test
	void customizerAppliedAfterProperties() {
		var pulsarProps = new PulsarProperties();
		var clientProps = pulsarProps.getClient();
		clientProps.setServiceUrl("foo");

		PulsarClientBuilderCustomizer customizer = (clientBuilder) -> clientBuilder.serviceUrl("bar");
		var configurer = new PulsarClientBuilderConfigurer(pulsarProps, List.of(customizer));
		var clientBuilder = mock(ClientBuilder.class);
		configurer.configure(clientBuilder);

		InOrder inOrder = inOrder(clientBuilder);
		inOrder.verify(clientBuilder).serviceUrl("foo");
		inOrder.verify(clientBuilder).serviceUrl("bar");
	}

	@Nested
	class AuthenticationProperties {

		private final String authPluginClassName = "org.apache.pulsar.client.impl.auth.AuthenticationToken";

		private final String authParamsStr = "{\"token\":\"1234\"}";

		private final String authToken = "1234";

		@Test
		void usingAuthParamsString() throws UnsupportedAuthenticationException {
			var pulsarProps = new PulsarProperties();
			var clientProps = pulsarProps.getClient();
			clientProps.setAuthPluginClassName(authPluginClassName);
			clientProps.setAuthParams(authParamsStr);
			var configurer = new PulsarClientBuilderConfigurer(pulsarProps, Collections.emptyList());
			var clientBuilder = mock(ClientBuilder.class);
			configurer.configure(clientBuilder);
			verify(clientBuilder).authentication(authPluginClassName, authParamsStr);
		}

		@Test
		void usingAuthenticationMap() throws UnsupportedAuthenticationException {
			var pulsarProps = new PulsarProperties();
			var clientProps = pulsarProps.getClient();
			clientProps.setAuthPluginClassName(authPluginClassName);
			clientProps.setAuthentication(Map.of("token", authToken));
			var configurer = new PulsarClientBuilderConfigurer(pulsarProps, Collections.emptyList());
			var clientBuilder = mock(ClientBuilder.class);
			configurer.configure(clientBuilder);
			verify(clientBuilder).authentication(authPluginClassName, authParamsStr);
		}

		@Test
		void notAllowedToUseBothAuthParamsStringAndAuthenticationMap() {
			var pulsarProps = new PulsarProperties();
			var clientProps = pulsarProps.getClient();
			clientProps.setAuthPluginClassName(authPluginClassName);
			clientProps.setAuthParams(authParamsStr);
			clientProps.setAuthentication(Map.of("token", authToken));
			var configurer = new PulsarClientBuilderConfigurer(pulsarProps, Collections.emptyList());
			var clientBuilder = mock(ClientBuilder.class);
			assertThatIllegalArgumentException().isThrownBy(() -> configurer.configure(clientBuilder))
					.withMessageContaining(
							"Cannot set both spring.pulsar.client.authParams and spring.pulsar.client.authentication.*");
		}

	}

}
