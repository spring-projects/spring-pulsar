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

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.pulsar.support.header.PulsarHeaderMapperTestUtil.assertSpringHeadersHavePulsarMetadata;
import static org.springframework.pulsar.support.header.PulsarHeaderMapperTestUtil.mockPulsarMessage;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import org.apache.pulsar.client.api.Message;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.springframework.lang.Nullable;
import org.springframework.messaging.MessageHeaders;
import org.springframework.pulsar.support.PulsarHeaders;

/**
 * Tests for {@link AbstractPulsarHeaderMapper}.
 *
 * @author Chris Bono
 */
abstract class AbstractPulsarHeaderMapperTests {

	abstract AbstractPulsarHeaderMapper<?, ?> mapper();

	abstract AbstractPulsarHeaderMapper<?, ?> mapperWithInboundPatterns(String... patterns);

	abstract AbstractPulsarHeaderMapper<?, ?> mapperWithOutboundPatterns(String... patterns);

	@Nested
	class ToSpringHeadersInboundTests {

		@Test
		void nullPulsarMessage() {
			assertThatNullPointerException().isThrownBy(() -> mapper().toSpringHeaders(null))
				.withMessage("pulsarMessage must not be null");
		}

		@Test
		void pulsarMessageWitRequiredMetadataOnly() {
			var pulsarMessage = mockPulsarMessage(false, Collections.emptyMap());
			var springHeaders = mapper().toSpringHeaders(pulsarMessage);
			assertSpringHeadersHavePulsarMetadata(springHeaders, pulsarMessage, false);
		}

		@Test
		void pulsarMessageWithAllMetadataAndSimpleUserProperties() {
			var pulsarHeaders = Collections.singletonMap("foo", "bar");
			var pulsarMessage = mockPulsarMessage(true, pulsarHeaders);
			var springHeaders = mapper().toSpringHeaders(pulsarMessage);
			assertSpringHeadersHavePulsarMetadata(springHeaders, pulsarMessage, true);
			assertThat(springHeaders).containsEntry("foo", "bar");
		}

		@Test
		void pulsarHeaderValuesFilteredByInboundPatterns() {
			var mapper = mapperWithInboundPatterns("!foo", "!" + PulsarHeaders.MESSAGE_ID, "*");
			var pulsarHeaders = new HashMap<String, String>();
			pulsarHeaders.put("foo", "bar");
			pulsarHeaders.put("info", "5150");
			var pulsarMessage = mockPulsarMessage(true, pulsarHeaders);
			var springHeaders = mapper.toSpringHeaders(pulsarMessage);
			// check metadata - sanity check metadata pulled in by existence of
			// MESSAGE_SIZE
			assertThat(springHeaders).containsKey(PulsarHeaders.MESSAGE_SIZE)
				.doesNotContainKey(PulsarHeaders.MESSAGE_ID);
			// check user properties
			assertThat(springHeaders).containsKey("info").doesNotContainKey("foo");
		}

		@Test
		void ensureCallbacksInvoked() {
			var inboundContext = "inboundContext";
			TestPulsarHeaderMapper testMapper = new TestPulsarHeaderMapper(inboundContext, null);
			TestPulsarHeaderMapper spyTestMapper = spy(testMapper);
			var pulsarHeaders = new HashMap<String, String>();
			pulsarHeaders.put("foo", "bar");
			pulsarHeaders.put("info", "5150");
			var pulsarMessage = mockPulsarMessage(true, pulsarHeaders);

			var springHeaders = spyTestMapper.toSpringHeaders(pulsarMessage);

			verify(spyTestMapper).toSpringHeadersOnStarted(pulsarMessage);
			verify(spyTestMapper).toSpringHeaderValue("foo", "bar", inboundContext);
			verify(spyTestMapper).toSpringHeaderValue("info", "5150", inboundContext);
			verify(spyTestMapper).toSpringHeadersOnCompleted(pulsarMessage, springHeaders, inboundContext);
			var numPulsarHeaders = springHeaders.size() - 2; // id/timestamp not pulsar
																// headers
			verify(spyTestMapper, times(numPulsarHeaders)).matchesForInbound(anyString());
		}

	}

	@Nested
	class ToPulsarHeadersOutboundTests {

		@Test
		void nullSpringHeaders() {
			assertThatNullPointerException().isThrownBy(() -> mapper().toPulsarHeaders(null))
				.withMessage("springHeaders must not be null");
		}

		@Test
		void emptySpringHeaders() {
			assertThat(mapper().toPulsarHeaders(new MessageHeaders(Collections.emptyMap()))).isEmpty();
		}

		@Test
		void springHeadersWithOnlyPulsarMetadata() {
			var pulsarMessage = mockPulsarMessage(true, Collections.emptyMap());
			var springHeaders = mapper().toSpringHeaders(pulsarMessage);
			assertSpringHeadersHavePulsarMetadata(springHeaders, pulsarMessage, true);
			assertThat(mapper().toPulsarHeaders(springHeaders)).isEmpty();
		}

		@Test
		void springHeadersWithSimpleValues() {
			var headers = new HashMap<String, Object>();
			headers.put("foo", "bar");
			assertThat(mapper().toPulsarHeaders(new MessageHeaders(headers))).containsOnly(entry("foo", "bar"));
		}

		@Test
		void springHeadersWithNullValue() {
			var headers = Collections.singletonMap("foo", null);
			assertThat(mapper().toPulsarHeaders(new MessageHeaders(headers))).containsEntry("foo", null);
		}

		@Test
		void springHeadersFilteredByOutboundPatterns() {
			var mapper = mapperWithOutboundPatterns("!foo", "*");
			var springHeaders = new HashMap<String, Object>();
			springHeaders.put("foo", "bar");
			springHeaders.put("info", "5150");
			assertThat(mapper.toPulsarHeaders(new MessageHeaders(springHeaders))).doesNotContainKey("foo")
				.containsKeys("id", "timestamp"); // '*' allows id/ts
		}

		@Test
		void springHeadersFilteredByOutboundPatternsIdAndTsExcluded() {
			var mapper = mapperWithOutboundPatterns("!foo", "info");
			var springHeaders = new HashMap<String, Object>();
			springHeaders.put("foo", "bar");
			springHeaders.put("info", "5150");
			assertThat(mapper.toPulsarHeaders(new MessageHeaders(springHeaders))).containsOnlyKeys("info");
		}

		@Test
		void springHeadersFilteredByOutboundPatterns_NoneAllowed() {
			var mapper = mapperWithOutboundPatterns("!foo");
			var springHeaders = new HashMap<String, Object>();
			springHeaders.put("foo", "bar");
			springHeaders.put("info", "5150");
			assertThat(mapper.toPulsarHeaders(new MessageHeaders(springHeaders))).isEmpty();
		}

		@Test
		void springHeadersCanAllowIdAndTimestampOutbound() {
			var mapper = mapperWithOutboundPatterns("id", "timestamp");
			var springHeaders = new HashMap<String, Object>();
			springHeaders.put("foo", "bar");
			assertThat(mapper.toPulsarHeaders(new MessageHeaders(springHeaders))).containsKeys("id", "timestamp")
				.doesNotContainKey("foo");
		}

		@Test
		void ensureCallbacksInvoked() {
			var outboundContext = "outboundContext";
			TestPulsarHeaderMapper testMapper = new TestPulsarHeaderMapper(null, outboundContext);
			TestPulsarHeaderMapper spyTestMapper = spy(testMapper);
			var headersMap = new HashMap<String, Object>();
			headersMap.put("foo", "bar");
			headersMap.put("info", "5150");
			var springHeaders = new MessageHeaders(headersMap);

			var pulsarHeaders = spyTestMapper.toPulsarHeaders(springHeaders);

			verify(spyTestMapper).toPulsarHeadersOnStarted(springHeaders);
			verify(spyTestMapper).toPulsarHeaderValue("foo", "bar", outboundContext);
			verify(spyTestMapper).toPulsarHeaderValue("info", "5150", outboundContext);
			verify(spyTestMapper).toPulsarHeadersOnCompleted(springHeaders, pulsarHeaders, outboundContext);
			verify(spyTestMapper, times(springHeaders.size())).matchesForOutbound(anyString());
		}

		@Test
		void neverMatchFiltersCanBeConfigured() {
			var mapper = mapperWithOutboundPatterns(PulsarHeaders.KEY, PulsarHeaders.MESSAGE_ID,
					PulsarHeaders.PRODUCER_NAME, "noSuchInternalHeader");
			var springHeaders = new HashMap<String, Object>();
			springHeaders.put(PulsarHeaders.KEY, "testKey");
			springHeaders.put(PulsarHeaders.KEY_BYTES, "testKeyBytes");
			springHeaders.put(PulsarHeaders.MESSAGE_ID, "testMsg");
			springHeaders.put(PulsarHeaders.PRODUCER_NAME, "testProducer");
			assertThat(mapper.toPulsarHeaders(new MessageHeaders(springHeaders))).containsOnlyKeys(PulsarHeaders.KEY,
					PulsarHeaders.MESSAGE_ID, PulsarHeaders.PRODUCER_NAME);
		}

	}

	@Nested
	class RoundtripTests {

		@Test
		void inboundToSpringOutboundToPulsarInboundToSpring() {
			// Inbound initial
			var customHeaders = new HashMap<String, String>();
			customHeaders.put("foo", "bar");
			var inboundPulsarMsg = mockPulsarMessage(true, customHeaders);
			// Comes into spring msg w/ headers (foo=bar) + pulsar msg metadata
			var inboundSpringHeaders = mapper().toSpringHeaders(inboundPulsarMsg);
			assertThat(inboundSpringHeaders).containsEntry("foo", "bar");
			assertSpringHeadersHavePulsarMetadata(inboundSpringHeaders, inboundPulsarMsg, true);
			// Modify spring msg headers -> 'foo=bar-bar' and add uuid=UUID(1234)
			var mutableSpringHeadersMap = new HashMap<>(inboundSpringHeaders);
			var fooValue = mutableSpringHeadersMap.get("foo");
			mutableSpringHeadersMap.put("foo", fooValue + "-" + fooValue);

			// Outbound
			var outboundPulsarHeaders = mapper().toPulsarHeaders(new MessageHeaders(mutableSpringHeadersMap));
			assertThat(outboundPulsarHeaders).containsEntry("foo", "bar-bar");

			// Inbound again: modified foo=bar-bar, UUID as object, pulsar msg metadata
			inboundPulsarMsg = mockPulsarMessage(true, outboundPulsarHeaders);
			inboundSpringHeaders = mapper().toSpringHeaders(inboundPulsarMsg);
			assertThat(inboundSpringHeaders).containsEntry("foo", "bar-bar");
			assertSpringHeadersHavePulsarMetadata(inboundSpringHeaders, inboundPulsarMsg, true);
		}

	}

	/**
	 * A test header mapper that helps w/ lifecycle callback verification.
	 */
	static class TestPulsarHeaderMapper extends AbstractPulsarHeaderMapper<String, String> {

		private String toSpringHeadersContext;

		private String toPulsarHeadersContext;

		TestPulsarHeaderMapper(String toSpringHeadersContext, String toPulsarHeadersContext) {
			super(List.of(), List.of());
			this.toSpringHeadersContext = toSpringHeadersContext;
			this.toPulsarHeadersContext = toPulsarHeadersContext;
		}

		@Nullable
		@Override
		protected String toSpringHeadersOnStarted(Message<?> pulsarMessage) {
			return this.toSpringHeadersContext;
		}

		@Override
		protected Object toSpringHeaderValue(String name, String value, @Nullable String context) {
			return value;
		}

		@Nullable
		@Override
		protected String toPulsarHeadersOnStarted(MessageHeaders springHeaders) {
			return this.toPulsarHeadersContext;
		}

		@Override
		protected String toPulsarHeaderValue(String name, Object value, @Nullable String context) {
			return Objects.toString(value, null);
		}

	}

}
