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

package org.springframework.pulsar.core;

import java.net.SocketAddress;

/**
 * @author Soby Chacko
 */
public class PulsarClientProperties {

	private String serviceUrl;

	private String authPluginClassName;

	private String authParams;

	private long operationTimeoutMs;

	private long statsIntervalSeconds;

	private int numIoThreads;

	private boolean useTcpNoDelay;

	private boolean useTls;

	private String tlsTrustCertsFilePath;

	private boolean tlsAllowInsecureConnection;

	private boolean tlsHostnameVerificationEnable;

	private int concurrentLookupRequest;

	private int maxLookupRequest;

	private int maxNumberOfRejectedRequestPerConnection;

	private int keepAliveIntervalSeconds;

	private int connectionTimeoutMs;

	private int requestTimeoutMs;

	private int defaultBackoffIntervalNanos;

	private long maxBackoffIntervalNanos;

	private SocketAddress socks5ProxyAddress;

	private String socks5ProxyUsername;

	private String socks5ProxyPassword;

	public String getServiceUrl() {
		return serviceUrl;
	}

	public void setServiceUrl(String serviceUrl) {
		this.serviceUrl = serviceUrl;
	}

	public String getAuthPluginClassName() {
		return authPluginClassName;
	}

	public void setAuthPluginClassName(String authPluginClassName) {
		this.authPluginClassName = authPluginClassName;
	}

	public String getAuthParams() {
		return authParams;
	}

	public void setAuthParams(String authParams) {
		this.authParams = authParams;
	}

	public long getOperationTimeoutMs() {
		return operationTimeoutMs;
	}

	public void setOperationTimeoutMs(long operationTimeoutMs) {
		this.operationTimeoutMs = operationTimeoutMs;
	}

	public long getStatsIntervalSeconds() {
		return statsIntervalSeconds;
	}

	public void setStatsIntervalSeconds(long statsIntervalSeconds) {
		this.statsIntervalSeconds = statsIntervalSeconds;
	}

	public int getNumIoThreads() {
		return numIoThreads;
	}

	public void setNumIoThreads(int numIoThreads) {
		this.numIoThreads = numIoThreads;
	}

	public boolean isUseTcpNoDelay() {
		return useTcpNoDelay;
	}

	public void setUseTcpNoDelay(boolean useTcpNoDelay) {
		this.useTcpNoDelay = useTcpNoDelay;
	}

	public boolean isUseTls() {
		return useTls;
	}

	public void setUseTls(boolean useTls) {
		this.useTls = useTls;
	}

	public String getTlsTrustCertsFilePath() {
		return tlsTrustCertsFilePath;
	}

	public void setTlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
		this.tlsTrustCertsFilePath = tlsTrustCertsFilePath;
	}

	public boolean isTlsAllowInsecureConnection() {
		return tlsAllowInsecureConnection;
	}

	public void setTlsAllowInsecureConnection(boolean tlsAllowInsecureConnection) {
		this.tlsAllowInsecureConnection = tlsAllowInsecureConnection;
	}

	public boolean isTlsHostnameVerificationEnable() {
		return tlsHostnameVerificationEnable;
	}

	public void setTlsHostnameVerificationEnable(boolean tlsHostnameVerificationEnable) {
		this.tlsHostnameVerificationEnable = tlsHostnameVerificationEnable;
	}

	public int getConcurrentLookupRequest() {
		return concurrentLookupRequest;
	}

	public void setConcurrentLookupRequest(int concurrentLookupRequest) {
		this.concurrentLookupRequest = concurrentLookupRequest;
	}

	public int getMaxLookupRequest() {
		return maxLookupRequest;
	}

	public void setMaxLookupRequest(int maxLookupRequest) {
		this.maxLookupRequest = maxLookupRequest;
	}

	public int getMaxNumberOfRejectedRequestPerConnection() {
		return maxNumberOfRejectedRequestPerConnection;
	}

	public void setMaxNumberOfRejectedRequestPerConnection(int maxNumberOfRejectedRequestPerConnection) {
		this.maxNumberOfRejectedRequestPerConnection = maxNumberOfRejectedRequestPerConnection;
	}

	public int getKeepAliveIntervalSeconds() {
		return keepAliveIntervalSeconds;
	}

	public void setKeepAliveIntervalSeconds(int keepAliveIntervalSeconds) {
		this.keepAliveIntervalSeconds = keepAliveIntervalSeconds;
	}

	public int getConnectionTimeoutMs() {
		return connectionTimeoutMs;
	}

	public void setConnectionTimeoutMs(int connectionTimeoutMs) {
		this.connectionTimeoutMs = connectionTimeoutMs;
	}

	public int getRequestTimeoutMs() {
		return requestTimeoutMs;
	}

	public void setRequestTimeoutMs(int requestTimeoutMs) {
		this.requestTimeoutMs = requestTimeoutMs;
	}

	public int getDefaultBackoffIntervalNanos() {
		return defaultBackoffIntervalNanos;
	}

	public void setDefaultBackoffIntervalNanos(int defaultBackoffIntervalNanos) {
		this.defaultBackoffIntervalNanos = defaultBackoffIntervalNanos;
	}

	public long getMaxBackoffIntervalNanos() {
		return maxBackoffIntervalNanos;
	}

	public void setMaxBackoffIntervalNanos(long maxBackoffIntervalNanos) {
		this.maxBackoffIntervalNanos = maxBackoffIntervalNanos;
	}

	public SocketAddress getSocks5ProxyAddress() {
		return socks5ProxyAddress;
	}

	public void setSocks5ProxyAddress(SocketAddress socks5ProxyAddress) {
		this.socks5ProxyAddress = socks5ProxyAddress;
	}

	public String getSocks5ProxyUsername() {
		return socks5ProxyUsername;
	}

	public void setSocks5ProxyUsername(String socks5ProxyUsername) {
		this.socks5ProxyUsername = socks5ProxyUsername;
	}

	public String getSocks5ProxyPassword() {
		return socks5ProxyPassword;
	}

	public void setSocks5ProxyPassword(String socks5ProxyPassword) {
		this.socks5ProxyPassword = socks5ProxyPassword;
	}
}
