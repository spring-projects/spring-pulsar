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

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to map Pulsar auth parameters to well-known keys.
 *
 * @author Alexander Preuß
 */
public final class WellKnownAuthParameters {

	private static final Map<String, String> lowerCaseToCamelCase = new HashMap<>();

	private static final String TENANT_DOMAIN = "tenantDomain";

	private static final String TENANT_SERVICE = "tenantService";

	private static final String PROVIDER_DOMAIN = "providerDomain";

	private static final String PRIVATE_KEY = "privateKey";

	private static final String PRIVATE_KEY_PATH = "privateKeyPath";

	private static final String KEY_ID = "keyId";

	private static final String AUTO_PREFETCH_ENABLED = "autoPrefetchEnabled";

	private static final String ATHENZ_CONF_PATH = "athenzConfPath";

	private static final String PRINCIPAL_HEADER = "principalHeader";

	private static final String ROLE_HEADER = "roleHeader";

	private static final String ZTS_URL = "ztsUrl";

	private static final String USER_ID = "userId";

	private static final String PASSWORD = "password";

	private static final String KEY_STORE_TYPE = "keyStoreType";

	private static final String KEY_STORE_PATH = "keyStorePath";

	private static final String KEY_STORE_PASSWORD = "keyStorePassword";

	private static final String TYPE = "type";

	private static final String ISSUER_URL = "issuerUrl";

	private static final String AUDIENCE = "audience";

	private static final String SCOPE = "scope";

	private static final String SASL_JAAS_CLIENT_SECTION_NAME = "saslJaasClientSectionName";

	private static final String SERVER_TYPE = "serverType";

	private static final String TLS_CERT_FILE = "tlsCertFile";

	private static final String TLS_KEY_FILE = "tlsKeyFile";

	private static final String TOKEN = "token";

	static {
		// Athenz
		addLowerCaseToCamelCaseMapping(TENANT_DOMAIN);
		addLowerCaseToCamelCaseMapping(TENANT_SERVICE);
		addLowerCaseToCamelCaseMapping(PROVIDER_DOMAIN);
		addLowerCaseToCamelCaseMapping(PRIVATE_KEY);
		addLowerCaseToCamelCaseMapping(PRIVATE_KEY_PATH);
		addLowerCaseToCamelCaseMapping(KEY_ID);
		addLowerCaseToCamelCaseMapping(AUTO_PREFETCH_ENABLED);
		addLowerCaseToCamelCaseMapping(ATHENZ_CONF_PATH);
		addLowerCaseToCamelCaseMapping(PRINCIPAL_HEADER);
		addLowerCaseToCamelCaseMapping(ROLE_HEADER);
		addLowerCaseToCamelCaseMapping(ZTS_URL);
		// Basic
		addLowerCaseToCamelCaseMapping(USER_ID);
		addLowerCaseToCamelCaseMapping(PASSWORD);
		// KeyStoreTls
		addLowerCaseToCamelCaseMapping(KEY_STORE_TYPE);
		addLowerCaseToCamelCaseMapping(KEY_STORE_PATH);
		addLowerCaseToCamelCaseMapping(KEY_STORE_PASSWORD);
		// OAuth2
		addLowerCaseToCamelCaseMapping(TYPE);
		addLowerCaseToCamelCaseMapping(ISSUER_URL);
		addLowerCaseToCamelCaseMapping(PRIVATE_KEY);
		addLowerCaseToCamelCaseMapping(AUDIENCE);
		addLowerCaseToCamelCaseMapping(SCOPE);
		// Sasl
		addLowerCaseToCamelCaseMapping(SASL_JAAS_CLIENT_SECTION_NAME);
		addLowerCaseToCamelCaseMapping(SERVER_TYPE);
		// Tls
		addLowerCaseToCamelCaseMapping(TLS_CERT_FILE);
		addLowerCaseToCamelCaseMapping(TLS_KEY_FILE);
		// Token
		addLowerCaseToCamelCaseMapping(TOKEN);
	}

	private WellKnownAuthParameters() {

	}

	/**
	 * Returns the camel-cased version a Pulsar auth parameter or the given key in case it
	 * is not part of the well-known ones.
	 * @param lowerCaseKey the lower-cased auth parameter
	 * @return the camel-cased auth parameter, or the lowerCaseKey if the parameter is not
	 * found.
	 */
	public static String getCamelCaseKey(String lowerCaseKey) {
		return lowerCaseToCamelCase.getOrDefault(lowerCaseKey, lowerCaseKey);
	}

	private static void addLowerCaseToCamelCaseMapping(String camelCaseKey) {
		lowerCaseToCamelCase.put(camelCaseKey.toLowerCase(), camelCaseKey);
	}

}
