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

	static {
		// Athenz
		lowerCaseToCamelCase.put("tenantdomain", "tenantDomain");
		lowerCaseToCamelCase.put("tenantservice", "tenantService");
		lowerCaseToCamelCase.put("providerdomain", "providerDomain");
		lowerCaseToCamelCase.put("privatekey", "privateKey");
		lowerCaseToCamelCase.put("privatekeypath", "privateKeyPath");
		lowerCaseToCamelCase.put("keyid", "keyId");
		lowerCaseToCamelCase.put("autoprefetchenabled", "autoPrefetchEnabled");
		lowerCaseToCamelCase.put("athenzconfpath", "athenzConfPath");
		lowerCaseToCamelCase.put("principalheader", "principalHeader");
		lowerCaseToCamelCase.put("roleheader", "roleHeader");
		lowerCaseToCamelCase.put("ztsurl", "ztsUrl");
		// Basic
		lowerCaseToCamelCase.put("userid", "userId");
		lowerCaseToCamelCase.put("password", "password");
		// KeyStoreTls
		lowerCaseToCamelCase.put("keystoretype", "keyStoreType");
		lowerCaseToCamelCase.put("keystorepath", "keyStorePath");
		lowerCaseToCamelCase.put("keystorepassword", "keyStorePassword");
		// OAuth2
		lowerCaseToCamelCase.put("type", "type");
		lowerCaseToCamelCase.put("issuerurl", "issuerUrl");
		lowerCaseToCamelCase.put("privatekey", "privateKey");
		lowerCaseToCamelCase.put("audience", "audience");
		lowerCaseToCamelCase.put("scope", "scope");
		// Sasl
		lowerCaseToCamelCase.put("sasljaasclientsectionname", "saslJaasClientSectionName");
		lowerCaseToCamelCase.put("servertype", "serverType");
		// Tls
		lowerCaseToCamelCase.put("tlscertfile", "tlsCertFile");
		lowerCaseToCamelCase.put("tlskeyfile", "tlsKeyFile");
		// Token
		lowerCaseToCamelCase.put("token", "token");
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

}
