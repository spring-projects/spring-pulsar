/*
 * Copyright 2023-present the original author or authors.
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

package org.springframework.pulsar.support.header;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

/**
 * A {@link PulsarHeaderMapper} that converts header values using simple 'toString'.
 *
 * @author Chris Bono
 */
public class ToStringPulsarHeaderMapper extends AbstractPulsarHeaderMapper<Object, Object> {

	/**
	 * Construct a 'ToString' mapper that will match all inbound headers and all outbound
	 * headers (except internal framework headers and id/timestamp).
	 */
	public ToStringPulsarHeaderMapper() {
		super(Collections.emptyList(), Collections.emptyList());
	}

	/**
	 * Construct a 'ToString' mapper that will match the supplied inbound and outbound
	 * patterns.
	 * @param inboundPatterns the inbound patterns to match - or empty map to match all
	 * @param outboundPatterns the outbound patterns to match - or empty to match all
	 * (except internal framework headers and id/timestamp)
	 */
	public ToStringPulsarHeaderMapper(List<String> inboundPatterns, List<String> outboundPatterns) {
		super(inboundPatterns, outboundPatterns);
	}

	@Override
	protected @Nullable String toPulsarHeaderValue(String name, @Nullable Object value, @Nullable Object context) {
		return Objects.toString(value, null);
	}

	@Override
	protected Object toSpringHeaderValue(String headerName, String rawHeader, @Nullable Object context) {
		return rawHeader;
	}

}
