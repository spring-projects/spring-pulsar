/*
 * Copyright 2018-2022 the original author or authors.
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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.apache.pulsar.client.api.Message;
import org.jspecify.annotations.Nullable;

import org.springframework.core.log.LogAccessor;
import org.springframework.messaging.MessageHeaders;
import org.springframework.pulsar.support.PulsarHeaders;
import org.springframework.pulsar.support.header.PulsarHeaderMatcher.NeverMatch;
import org.springframework.pulsar.support.header.PulsarHeaderMatcher.PatternMatch;

/**
 * Base {@code PulsarHeaderMapper} implementation that constrains which headers are mapped
 * via {@link PulsarHeaderMatcher header matchers}.
 *
 * <p>
 * Concrete implementations only need to specify how to convert a header value to and from
 * Spring Messaging and Pulsar, by implementing the abstract {@link #toPulsarHeaderValue}
 * and {@link #toSpringHeaderValue} methods.
 *
 * @param <ToPulsarHeadersContextType> type of context object used in the
 * {@link #toPulsarHeaders} API
 * @param <ToSpringHeadersContextType> type of context object used in the
 * {@link #toSpringHeaders} API
 * @author Chris Bono
 */
public abstract class AbstractPulsarHeaderMapper<ToPulsarHeadersContextType, ToSpringHeadersContextType>
		implements PulsarHeaderMapper {

	private static final PatternMatch EXCLUDE_PATTERN_ID = PatternMatch.fromPatternString("!id");

	private static final PatternMatch EXCLUDE_PATTERN_TIMESTAMP = PatternMatch.fromPatternString("!timestamp");

	private static final List<String> NEVER_MATCH_OUTBOUND_INTERNAL_HEADERS = List.of(PulsarHeaders.KEY,
			PulsarHeaders.KEY_BYTES, PulsarHeaders.ORDERING_KEY, PulsarHeaders.INDEX, PulsarHeaders.MESSAGE_ID,
			PulsarHeaders.BROKER_PUBLISH_TIME, PulsarHeaders.EVENT_TIME, PulsarHeaders.MESSAGE_SIZE,
			PulsarHeaders.PRODUCER_NAME, PulsarHeaders.RAW_DATA, PulsarHeaders.PUBLISH_TIME,
			PulsarHeaders.REDELIVERY_COUNT, PulsarHeaders.REPLICATED_FROM, PulsarHeaders.SCHEMA_VERSION,
			PulsarHeaders.SEQUENCE_ID, PulsarHeaders.TOPIC_NAME);

	protected final LogAccessor logger = new LogAccessor(this.getClass());

	private final List<PulsarHeaderMatcher> inboundMatchers = new ArrayList<>();

	private final List<PulsarHeaderMatcher> outboundMatchers = new ArrayList<>();

	/**
	 * Construct a mapper that will match the supplied inbound and outbound patterns.
	 * <p>
	 * <strong>NOTE:</strong> By default, internal framework headers and the {@code "id"}
	 * and {@code "timestamp"} headers are <em>not</em> mapped outbound but can be
	 * included by adding them to {@code outboundPatterns}.
	 * <p>
	 * <strong>NOTE:</strong> The patterns are applied in order, stopping on the first
	 * match (positive or negative). When no pattern is specified, the {@code "*"} pattern
	 * is added last. However, once a pattern is specified, the {@code "*"} is not added
	 * and must be added to the specified patterns if desired.
	 * @param inboundPatterns the inbound patterns to match - or empty to match all
	 * @param outboundPatterns the outbound patterns to match - or empty to match all
	 * (except internal framework headers and id/timestamp)
	 *
	 * @see org.springframework.util.PatternMatchUtils#simpleMatch(String, String)
	 */
	public AbstractPulsarHeaderMapper(List<String> inboundPatterns, List<String> outboundPatterns) {
		Objects.requireNonNull(inboundPatterns, "inboundPatterns must be specified");
		Objects.requireNonNull(outboundPatterns, "outboundPatterns must be specified");
		inboundPatterns.forEach((p) -> this.inboundMatchers.add(PatternMatch.fromPatternString(p)));
		// @formatter:off
		this.outboundMatchers.add(getNeverMatch(outboundPatterns));
		// @formatter:on
		if (outboundPatterns.isEmpty()) {
			this.outboundMatchers.add(EXCLUDE_PATTERN_ID);
			this.outboundMatchers.add(EXCLUDE_PATTERN_TIMESTAMP);
			this.outboundMatchers.add(PatternMatch.fromPatternString("*"));
		}
		else {
			outboundPatterns.forEach((p) -> this.outboundMatchers.add(PatternMatch.fromPatternString(p)));
			this.outboundMatchers.add(EXCLUDE_PATTERN_ID);
			this.outboundMatchers.add(EXCLUDE_PATTERN_TIMESTAMP);
		}
	}

	private NeverMatch getNeverMatch(List<String> outboundPatterns) {
		List<String> neverMatches = new ArrayList<>(NEVER_MATCH_OUTBOUND_INTERNAL_HEADERS);
		neverMatches.removeAll(outboundPatterns);
		return new NeverMatch(neverMatches.toArray(new String[0]));
	}

	@Override
	public Map<String, String> toPulsarHeaders(MessageHeaders springHeaders) {
		Objects.requireNonNull(springHeaders, "springHeaders must not be null");
		var pulsarHeaders = new LinkedHashMap<String, String>();
		var context = toPulsarHeadersOnStarted(springHeaders);
		springHeaders.forEach((key, rawValue) -> {
			if (matchesForOutbound(key)) {
				var value = toPulsarHeaderValue(key, rawValue, context);
				pulsarHeaders.put(key, value);
			}
		});
		toPulsarHeadersOnCompleted(springHeaders, pulsarHeaders, context);
		return pulsarHeaders;
	}

	/**
	 * Called at the beginning of every {@link #toPulsarHeaders} invocation and the
	 * returned value is passed into each {@link #toPulsarHeaderValue} invocation as well
	 * as the final {@link #toPulsarHeadersOnCompleted}. Allows concrete implementations
	 * the ability to create an arbitrary context object and have it passed through the
	 * mapping process.
	 * @param springHeaders the Spring Messaging headers that are being converted
	 * @return optional context to pass through the mapping invocation
	 */
	@Nullable protected ToPulsarHeadersContextType toPulsarHeadersOnStarted(MessageHeaders springHeaders) {
		return null;
	}

	/**
	 * Determine the Pulsar header value to use for a Spring Messaging header.
	 * @param name the Spring Messaging header name
	 * @param value the Spring Messaging header value
	 * @param context the optional context used for the mapping invocation
	 * @return the Pulsar header value to use
	 */
	protected abstract String toPulsarHeaderValue(String name, Object value,
			@Nullable ToPulsarHeadersContextType context);

	/**
	 * Called at the end of a successful {@link #toSpringHeaders} invocation. Allows
	 * concrete implementations the ability to do final logic/cleanup.
	 * @param springHeaders the Spring Messaging headers that were mapped
	 * @param pulsarHeaders the resulting map of Pulsar message headers
	 * @param context the optional context used for the mapping invocation
	 */
	protected void toPulsarHeadersOnCompleted(MessageHeaders springHeaders, Map<String, String> pulsarHeaders,
			@Nullable ToPulsarHeadersContextType context) {
	}

	@Override
	public MessageHeaders toSpringHeaders(Message<?> pulsarMessage) {
		Objects.requireNonNull(pulsarMessage, "pulsarMessage must not be null");
		var context = toSpringHeadersOnStarted(pulsarMessage);
		var headersMap = new LinkedHashMap<String, Object>();

		// custom user properties (headers)
		pulsarMessage.getProperties().forEach((name, value) -> {
			if (matchesForInbound(name)) {
				var valueToUse = toSpringHeaderValue(name, value, context);
				headersMap.put(name, valueToUse);
			}
		});
		// built-in Pulsar metadata headers
		if (pulsarMessage.hasKey()) {
			addToHeadersMapIfAllowed(PulsarHeaders.KEY, pulsarMessage::getKey, headersMap::put);
			addToHeadersMapIfAllowed(PulsarHeaders.KEY_BYTES, pulsarMessage::getKeyBytes, headersMap::put);
		}
		if (pulsarMessage.hasOrderingKey()) {
			addToHeadersMapIfAllowed(PulsarHeaders.ORDERING_KEY, pulsarMessage::getOrderingKey, headersMap::put);
		}
		if (pulsarMessage.hasIndex()) {
			addToHeadersMapIfAllowed(PulsarHeaders.INDEX, pulsarMessage::getIndex, headersMap::put);
		}
		addToHeadersMapIfAllowed(PulsarHeaders.MESSAGE_ID, pulsarMessage::getMessageId, headersMap::put);
		addToHeadersMapIfAllowed(PulsarHeaders.BROKER_PUBLISH_TIME, pulsarMessage::getBrokerPublishTime,
				headersMap::put);
		addToHeadersMapIfAllowed(PulsarHeaders.EVENT_TIME, pulsarMessage::getEventTime, headersMap::put);
		addToHeadersMapIfAllowed(PulsarHeaders.MESSAGE_SIZE, pulsarMessage::size, headersMap::put);
		addToHeadersMapIfAllowed(PulsarHeaders.PRODUCER_NAME, pulsarMessage::getProducerName, headersMap::put);
		addToHeadersMapIfAllowed(PulsarHeaders.RAW_DATA, pulsarMessage::getData, headersMap::put);
		addToHeadersMapIfAllowed(PulsarHeaders.PUBLISH_TIME, pulsarMessage::getPublishTime, headersMap::put);
		addToHeadersMapIfAllowed(PulsarHeaders.REDELIVERY_COUNT, pulsarMessage::getRedeliveryCount, headersMap::put);
		addToHeadersMapIfAllowed(PulsarHeaders.REPLICATED_FROM, pulsarMessage::getReplicatedFrom, headersMap::put);
		addToHeadersMapIfAllowed(PulsarHeaders.SCHEMA_VERSION, pulsarMessage::getSchemaVersion, headersMap::put);
		addToHeadersMapIfAllowed(PulsarHeaders.SEQUENCE_ID, pulsarMessage::getSequenceId, headersMap::put);
		addToHeadersMapIfAllowed(PulsarHeaders.TOPIC_NAME, pulsarMessage::getTopicName, headersMap::put);

		var springHeaders = new MessageHeaders(headersMap);
		toSpringHeadersOnCompleted(pulsarMessage, springHeaders, context);
		return springHeaders;
	}

	/**
	 * Called at the beginning of every {@link #toSpringHeaders} invocation and the
	 * returned value is passed into each {@link #toSpringHeaderValue} invocation as well
	 * as the final {@link #toSpringHeadersOnCompleted}. Allows concrete implementations
	 * the ability to create an arbitrary context object and have it passed through the
	 * mapping process.
	 * @param pulsarMessage the Pulsar message whose headers are being mapped
	 * @return optional context to pass through the mapping invocation
	 */
	@Nullable protected ToSpringHeadersContextType toSpringHeadersOnStarted(Message<?> pulsarMessage) {
		return null;
	}

	/**
	 * Determine the Spring Messaging header value to use for a Pulsar header.
	 * @param name the Pulsar header name
	 * @param value the Pulsar header value
	 * @param context the optional context used for the mapping invocation
	 * @return the Spring Messaging header value to use
	 */
	protected abstract Object toSpringHeaderValue(String name, String value,
			@Nullable ToSpringHeadersContextType context);

	/**
	 * Called at the end of a successful {@link #toPulsarHeaders} invocation. Allows
	 * concrete implementations the ability to do final logic/cleanup.
	 * @param pulsarMessage the Pulsar message whose headers were mapped
	 * @param springHeaders the resulting Spring Messaging headers
	 * @param context the optional context used for the mapping invocation
	 */
	protected void toSpringHeadersOnCompleted(Message<?> pulsarMessage, MessageHeaders springHeaders,
			@Nullable ToSpringHeadersContextType context) {
	}

	/**
	 * Determine if a header name matches any of the currently configured list of outbound
	 * matchers.
	 * @param header the name of the header to check
	 * @return whether the header matches for outbound
	 */
	protected boolean matchesForOutbound(String header) {
		if (this.outboundMatchers.isEmpty()) {
			return true;
		}
		return matchesAny(header, this.outboundMatchers);
	}

	/**
	 * Determine if a header name matches any of the currently configured list of inbound
	 * matchers.
	 * @param header the name of the header to check
	 * @return whether the header matches for inbound
	 */
	protected boolean matchesForInbound(String header) {
		if (this.inboundMatchers.isEmpty()) {
			return true;
		}
		return matchesAny(header, this.inboundMatchers);
	}

	private boolean matchesAny(String header, List<PulsarHeaderMatcher> matchers) {
		for (PulsarHeaderMatcher matcher : matchers) {
			if (matcher.matchHeader(header)) {
				return !matcher.isNegated();
			}
		}
		this.logger.debug(() -> "header (%s) WILL NOT be mapped; matched no patterns".formatted(header));
		return false;
	}

	private void addToHeadersMapIfAllowed(String key, Supplier<Object> valueSupplier,
			BiConsumer<String, Object> mapConsumer) {
		if (matchesForInbound(key)) {
			mapConsumer.accept(key, valueSupplier.get());
		}
	}

}
