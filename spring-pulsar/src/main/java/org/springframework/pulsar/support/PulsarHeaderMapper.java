/*
 * Copyright 2022-2023 the original author or authors.
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

package org.springframework.pulsar.support;

import java.util.Map;

import org.apache.pulsar.client.api.Message;

import org.springframework.messaging.MessageHeaders;

/**
 * Defines the contract for mapping Spring Messaging {@link MessageHeaders} to and from
 * Pulsar message headers.
 * <p>
 * <b>NOTE:</b>Pulsar does not have the concept of message headers, but rather message
 * metadata. The terms &quot;Pulsar message headers&quot; and &quot;Pulsar message
 * metadata&quot; are used interchangeably.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public interface PulsarHeaderMapper {

	/**
	 * Map from the given Spring Messaging headers to Pulsar message headers.
	 * @param springHeaders the Spring messaging headers
	 * @return map of Pulsar message headers or an empty map for no headers.
	 */
	Map<String, String> fromSpringHeaders(MessageHeaders springHeaders);

	/**
	 * Map the headers from the given Pulsar message to Spring Messaging headers.
	 * @param pulsarMessage the Pulsar message containing the headers to map
	 * @return the Spring Messaging headers
	 */
	MessageHeaders toSpringHeaders(Message<?> pulsarMessage);

}
