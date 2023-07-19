/*
 * Copyright 2023 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.pulsar.support.header.JsonPulsarHeaderMapper.JSON_TYPES;
import static org.springframework.pulsar.support.header.PulsarHeaderMapperTestUtil.assertSpringHeadersHavePulsarMetadata;
import static org.springframework.pulsar.support.header.PulsarHeaderMapperTestUtil.mockPulsarMessage;

import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.messaging.MessageHeaders;
import org.springframework.pulsar.support.header.JsonPulsarHeaderMapper.NonTrustedHeaderType;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * Tests for {@link JsonPulsarHeaderMapper}.
 *
 * @author Chris Bono
 */
class JsonPulsarHeaderMapperTests extends AbstractPulsarHeaderMapperTests {

	@Override
	AbstractPulsarHeaderMapper<?, ?> mapper() {
		return JsonPulsarHeaderMapper.builder().build();
	}

	@Override
	AbstractPulsarHeaderMapper<?, ?> mapperWithInboundPatterns(String... patterns) {
		return JsonPulsarHeaderMapper.builder().inboundPatterns(patterns).build();
	}

	@Override
	AbstractPulsarHeaderMapper<?, ?> mapperWithOutboundPatterns(String... patterns) {
		return JsonPulsarHeaderMapper.builder().outboundPatterns(patterns).build();
	}

	@Nested
	class ToSpringHeadersJsonMapperSpecificInboundTests {

		@Test
		void pulsarMessageWithObjectUserProperty() {
			var uuid = UUID.randomUUID();
			var customHeaders = new HashMap<String, String>();
			customHeaders.put("uuid", "\"%s\"".formatted(uuid.toString()));
			customHeaders.put(JSON_TYPES, "{\"uuid\":\"%s\"}".formatted(UUID.class.getName()));
			var pulsarMessage = mockPulsarMessage(true, customHeaders);
			var springHeaders = mapper().toSpringHeaders(pulsarMessage);
			assertThat(springHeaders).containsEntry("uuid", uuid);
		}

		@Test
		void pulsarMessageWithUntrustedUserProperty() {
			// Once trustedPackages is set, all others are untrusted
			var mapper = JsonPulsarHeaderMapper.builder().trustedPackages("com.acme").build();
			var uuid = UUID.randomUUID();
			var serializedUuid = "\"%s\"".formatted(uuid.toString());
			var customHeaders = new HashMap<String, String>();
			customHeaders.put("uuid", serializedUuid);
			customHeaders.put(JSON_TYPES, "{\"uuid\":\"%s\"}".formatted(UUID.class.getName()));
			var pulsarMessage = mockPulsarMessage(true, customHeaders);
			var springHeaders = mapper.toSpringHeaders(pulsarMessage);
			assertThat(springHeaders).containsEntry("uuid",
					new NonTrustedHeaderType(serializedUuid, UUID.class.getName()));
		}

		@ParameterizedTest
		@ValueSource(booleans = { true, false })
		void pulsarMessageWithUpstreamNth(boolean nowTrusted) throws JsonProcessingException {
			var mapper = JsonPulsarHeaderMapper.builder()
				.trustedPackages(nowTrusted ? UUID.class.getPackageName() : "com.acme")
				.build();
			var uuid = UUID.randomUUID();
			var serializedUuid = "\"%s\"".formatted(uuid.toString());
			var upstreamNth = new NonTrustedHeaderType(serializedUuid, UUID.class.getName());
			var serializedUpstreamNth = JacksonUtils.enhancedObjectMapper().writeValueAsString(upstreamNth);
			var customHeaders = new HashMap<String, String>();
			customHeaders.put("uuid", serializedUpstreamNth);
			customHeaders.put(JSON_TYPES, "{\"uuid\":\"%s\"}".formatted(NonTrustedHeaderType.class.getName()));
			var pulsarMessage = mockPulsarMessage(true, customHeaders);
			var springHeaders = mapper.toSpringHeaders(pulsarMessage);
			assertThat(springHeaders).containsEntry("uuid", nowTrusted ? uuid : upstreamNth);
		}

	}

	@Nested
	class ToPulsarHeadersJsonMapperSpecificOutboundTests {

		@Test
		void springHeadersWithObjectValues() {
			var uuid = UUID.randomUUID();
			var headers = new HashMap<String, Object>();
			headers.put("foo", "bar");
			headers.put("uuid", uuid);
			assertThat(mapper().toPulsarHeaders(new MessageHeaders(headers))).containsEntry("foo", "bar")
				.containsEntry("uuid", "\"%s\"".formatted(uuid.toString()))
				.extractingByKey(JSON_TYPES, InstanceOfAssertFactories.STRING)
				.contains("\"uuid\":\"java.util.UUID\"");
		}

		@Test
		void springHeadersWithValuesInToStringClasses() {
			var mapper = JsonPulsarHeaderMapper.builder().toStringClasses(UUID.class.getName()).build();
			var uuid = UUID.randomUUID();
			var headers = Collections.<String, Object>singletonMap("uuid", uuid);
			assertThat(mapper.toPulsarHeaders(new MessageHeaders(headers))).containsEntry("uuid", uuid.toString())
				.doesNotContainKey(JSON_TYPES);
		}

	}

	@Nested
	class RoundtripJsonMapperSpecificTests {

		@Test
		void inboundToSpringOutboundToPulsarInboundToSpring() {
			var mapper = mapper();
			// Inbound initial
			var uuid = UUID.randomUUID();
			var customHeaders = new HashMap<String, String>();
			customHeaders.put("foo", "bar");
			var inboundPulsarMsg = mockPulsarMessage(true, customHeaders);
			// Comes into spring msg w/ headers (foo=bar) + pulsar msg metadata
			var inboundSpringHeaders = mapper.toSpringHeaders(inboundPulsarMsg);
			assertThat(inboundSpringHeaders).containsEntry("foo", "bar");
			assertSpringHeadersHavePulsarMetadata(inboundSpringHeaders, inboundPulsarMsg, true);

			// Modify spring msg headers -> 'foo=bar-bar' and add uuid=UUID(1234)
			var mutableSpringHeadersMap = new HashMap<>(inboundSpringHeaders);
			var fooValue = mutableSpringHeadersMap.get("foo");
			mutableSpringHeadersMap.put("foo", fooValue + "-" + fooValue);
			mutableSpringHeadersMap.put("uuid", uuid);

			// Outbound
			var outboundPulsarHeaders = mapper.toPulsarHeaders(new MessageHeaders(mutableSpringHeadersMap));
			assertThat(outboundPulsarHeaders).containsEntry("foo", "bar-bar");
			assertThat(outboundPulsarHeaders).containsEntry("uuid", "\"%s\"".formatted(uuid.toString()));
			assertThat(outboundPulsarHeaders).containsOnlyKeys("foo", "uuid", JSON_TYPES);

			// Inbound again: modified foo=bar-bar, UUID as object, pulsar msg metadata
			inboundPulsarMsg = mockPulsarMessage(true, outboundPulsarHeaders);
			inboundSpringHeaders = mapper.toSpringHeaders(inboundPulsarMsg);
			assertThat(inboundSpringHeaders).containsEntry("foo", "bar-bar").containsEntry("uuid", uuid);
			assertSpringHeadersHavePulsarMetadata(inboundSpringHeaders, inboundPulsarMsg, true);
		}

		@Test
		void roundtripWithByteArrayValue() {
			var payload = "payload5150".getBytes();
			var payloadEnc = Base64.getEncoder().encodeToString(payload);
			var headers = Map.<String, Object>of("payload", payload);
			var pulsarHeaders = mapper().toPulsarHeaders(new MessageHeaders(headers));
			assertThat(pulsarHeaders).containsEntry("payload", "\"%s\"".formatted(payloadEnc))
				.containsEntry(JSON_TYPES, "{\"payload\":\"[B\"}");
			var pulsarMessage = mockPulsarMessage(true, pulsarHeaders);
			var springHeaders = mapper().toSpringHeaders(pulsarMessage);
			assertThat(springHeaders).containsEntry("payload", payload);
		}

	}

}
