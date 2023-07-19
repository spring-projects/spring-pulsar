/*
 * Copyright 2017-2022 the original author or authors.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.pulsar.client.api.Message;

import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.messaging.MessageHeaders;
import org.springframework.pulsar.support.header.JsonPulsarHeaderMapper.ToPulsarHeadersContext;
import org.springframework.pulsar.support.header.JsonPulsarHeaderMapper.ToSpringHeadersContext;
import org.springframework.util.ClassUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A {@code PulsarHeaderMapper} implementation that writes headers as JSON.
 *
 * <p>
 * Allows user to constrain which classes are {@code "trusted"} for deserialization as
 * well as configure a list of classes to serialize with a simple {@code "toString"}.
 *
 * @author Chris Bono
 */
public class JsonPulsarHeaderMapper extends AbstractPulsarHeaderMapper<ToPulsarHeadersContext, ToSpringHeadersContext> {

	private static final Set<String> TRUSTED_ARRAY_TYPES = new HashSet<>(
			Arrays.asList("[B", "[I", "[J", "[F", "[D", "[C"));

	private static final List<String> DEFAULT_TO_STRING_CLASSES = Arrays.asList("org.springframework.util.MimeType",
			"org.springframework.http.MediaType");

	/**
	 * Header name for java types of other headers.
	 */
	public static final String JSON_TYPES = "spring_json_header_types";

	private final ObjectMapper objectMapper;

	private final Set<String> trustedPackages = new LinkedHashSet<>();

	private final Set<String> toStringClasses = new LinkedHashSet<>(DEFAULT_TO_STRING_CLASSES);

	/**
	 * Construct an instance with the provided specifications.
	 * <p>
	 * <strong>NOTE:</strong> Internal framework headers are <em>never</em> mapped
	 * outbound. By default, the {@code "id"} and {@code "timestamp"} headers are also
	 * excluded from outbound mapping but can be included by adding them to
	 * {@code outboundPatterns}.
	 * <p>
	 * <strong>NOTE:</strong> The patterns are applied in order, stopping on the first
	 * match (positive or negative). When no pattern is specified, the {@code "*"} pattern
	 * is added last. However, once a pattern is specified, the {@code "*"} is not added
	 * and must be added to the specified patterns if desired.
	 * @param objectMapper the object mapper to use to read/write JSON
	 * @param inboundPatterns the inbound patterns to match - or empty to match all
	 * @param outboundPatterns the outbound patterns to match - or empty to match all
	 * (except internal framework headers and id/timestamp)
	 * @param trustedPackages the additional packages to trust (allow to be deserialized)
	 * @param toStringClasses the additional classes to use {@code toString} for
	 * serialization
	 *
	 * @see org.springframework.util.PatternMatchUtils#simpleMatch(String, String)
	 */
	JsonPulsarHeaderMapper(ObjectMapper objectMapper, List<String> inboundPatterns, List<String> outboundPatterns,
			Set<String> trustedPackages, Set<String> toStringClasses) {
		super(inboundPatterns, outboundPatterns);
		this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
		Objects.requireNonNull(trustedPackages, "trustedPackages must not be null");
		Objects.requireNonNull(toStringClasses, "toStringClasses must not be null");
		for (var trusted : trustedPackages) {
			if ("*".equals(trusted)) {
				this.trustedPackages.clear();
				break;
			}
			this.trustedPackages.add(trusted);
		}
		this.toStringClasses.addAll(toStringClasses);
	}

	/**
	 * Construct a builder instance that can be used to construct a
	 * {@link JsonPulsarHeaderMapper}.
	 * @return builder instance
	 */
	public static JsonPulsarHeaderMapperBuilder builder() {
		return new JsonPulsarHeaderMapperBuilder();
	}

	protected ObjectMapper getObjectMapper() {
		return this.objectMapper;
	}

	protected Set<String> getTrustedPackages() {
		return this.trustedPackages;
	}

	protected Set<String> getToStringClasses() {
		return this.toStringClasses;
	}

	// ToPulsarHeaders ------

	@Override
	protected ToPulsarHeadersContext toPulsarHeadersOnStarted(MessageHeaders springHeaders) {
		var jsonHeaders = new LinkedHashMap<String, String>();
		return new ToPulsarHeadersContext(jsonHeaders);
	}

	@Override
	protected String toPulsarHeaderValue(String name, Object rawValue, ToPulsarHeadersContext context) {
		if (rawValue == null) {
			return null;
		}
		if (rawValue instanceof String) {
			return (String) rawValue;
		}
		String className = rawValue.getClass().getName();
		if (this.toStringClasses.contains(className)) {
			return rawValue.toString();
		}
		try {
			var valueToAdd = getObjectMapper().writeValueAsString(rawValue);
			context.jsonTypes().put(name, className);
			return valueToAdd;
		}
		catch (Exception e) {
			logger.debug(e, () -> "Could not map %s with type %s (will instead map w/ toString()) reason: %s"
				.formatted(name, className, e.getMessage()));
		}
		return rawValue.toString();
	}

	@Override
	protected void toPulsarHeadersOnCompleted(MessageHeaders springHeaders, Map<String, String> pulsarHeaders,
			ToPulsarHeadersContext context) {
		var jsonHeaders = context.jsonTypes();
		if (jsonHeaders.size() > 0) {
			try {
				pulsarHeaders.put(JSON_TYPES, getObjectMapper().writeValueAsString(jsonHeaders));
			}
			catch (Exception e) {
				logger.error(e, () -> "Could not add json types header due to: %s".formatted(e.getMessage()));
			}
		}
	}

	// ToSpringHeaders ------

	@Override
	protected boolean matchesForInbound(String header) {
		return !header.equals(JSON_TYPES) && super.matchesForInbound(header);
	}

	@NonNull
	@Override
	protected ToSpringHeadersContext toSpringHeadersOnStarted(Message<?> pulsarMessage) {
		Map<String, String> types = new HashMap<>();
		if (pulsarMessage.hasProperty(JSON_TYPES)) {
			String jsonTypesStr = pulsarMessage.getProperty(JSON_TYPES);
			try {
				types = getObjectMapper().readValue(jsonTypesStr, new TypeReference<>() {
				});
			}
			catch (IOException e) {
				logger.error(e,
						() -> "Could not decode json types: %s due to: %s".formatted(jsonTypesStr, e.getMessage()));
			}
		}
		return new ToSpringHeadersContext(types);
	}

	@Override
	protected Object toSpringHeaderValue(String name, String value, ToSpringHeadersContext context) {
		var jsonTypes = context.jsonTypes();
		if (jsonTypes != null && jsonTypes.containsKey(name)) {
			String requestedType = jsonTypes.get(name);
			return toJsonHeaderValue(name, value, requestedType);
		}
		return value;
	}

	private Object toJsonHeaderValue(String name, String value, String requestedType) {
		if (!trusted(requestedType)) {
			return new NonTrustedHeaderType(value, requestedType);
		}

		final Class<?> type;
		try {
			type = ClassUtils.forName(requestedType, null);
		}
		catch (Exception e) {
			logger.error(e, () -> "Could not load type (%s) for header (%s) due to: %s".formatted(requestedType, name,
					e.getMessage()));
			return value;
		}

		try {
			return decodeValue(name, value, type);
		}
		catch (IOException e) {
			logger.error(e, () -> "Could not decode type (%s) for header (%s) using value (%s) due to: %s"
				.formatted(type, name, value, e.getMessage()));
		}
		return value;
	}

	private Object decodeValue(String name, String value, Class<?> type) throws IOException {
		Object decodedValue = getObjectMapper().readValue(value, type);
		if (!type.equals(NonTrustedHeaderType.class)) {
			return decodedValue;
		}
		// Upstream NTHT propagated; may be trusted here...
		NonTrustedHeaderType nth = (NonTrustedHeaderType) decodedValue;
		if (!trusted(nth.untrustedType())) {
			return nth;
		}
		try {
			decodedValue = getObjectMapper().readValue(nth.headerValue(),
					ClassUtils.forName(nth.untrustedType(), null));
		}
		catch (Exception e) {
			logger.error(e,
					() -> "Could not decode non-trusted header type (%s) for header (%s) using value (%s) due to: %s"
						.formatted(nth.untrustedType(), name, nth.headerValue(), e.getMessage()));
		}
		return decodedValue;
	}

	// Trusted ------

	protected boolean trusted(String requestedType) {
		if (requestedType.equals(NonTrustedHeaderType.class.getName())) {
			return true;
		}
		if (TRUSTED_ARRAY_TYPES.contains(requestedType)) {
			return true;
		}
		if (this.trustedPackages.isEmpty()) {
			return true;
		}
		var type = requestedType.startsWith("[") ? requestedType.substring(2) : requestedType;
		var lastDot = type.lastIndexOf('.');
		if (lastDot < 0) {
			return false;
		}
		var packageName = type.substring(0, lastDot);
		for (var trustedPackage : this.trustedPackages) {
			if (packageName.equals(trustedPackage) || packageName.startsWith(trustedPackage + ".")) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Represents a header that could not be decoded due to an untrusted type.
	 *
	 * @param headerValue the header value that could not be decoded
	 * @param untrustedType the class name of the encoded header value
	 */
	public record NonTrustedHeaderType(String headerValue, String untrustedType) {
	}

	/**
	 * Context used for {@link #toPulsarHeaders} (outbound) that includes the cumulative
	 * map of header name to classname for headers that are serialized with JSON. This map
	 * is ultimately included as a separate {@link #JSON_TYPES} header on the outgoing
	 * Pulsar message. This allows for the inbound consumer to decode the headers when/if
	 * the message is later consumed.
	 *
	 * @param jsonTypes cumulative map of header name to classname for types that are
	 * serialized as JSON
	 */
	public record ToPulsarHeadersContext(Map<String, String> jsonTypes) {
	}

	/**
	 * Context used for {@link #toSpringHeaders} (inbound) that includes the cumulative
	 * map of header name to classname for headers in the incoming message that were
	 * serialized with JSON. This is used to decode the headers in the incoming message.
	 *
	 * @param jsonTypes cumulative map of header name to classname for types that were
	 * serialized as JSON
	 */
	public record ToSpringHeadersContext(Map<String, String> jsonTypes) {
	}

	public static class JsonPulsarHeaderMapperBuilder {

		private ObjectMapper objectMapper;

		private final Set<String> trustedPackages = new HashSet<>();

		private final Set<String> toStringClasses = new HashSet<>();

		private final List<String> inboundPatterns = new ArrayList<>();

		private final List<String> outboundPatterns = new ArrayList<>();

		/**
		 * Sets the object mapper to use to read/write header values as JSON.
		 * @param objectMapper the object mapper
		 * @return current builder
		 */
		public JsonPulsarHeaderMapperBuilder objectMapper(@Nullable ObjectMapper objectMapper) {
			this.objectMapper = objectMapper;
			return this;
		}

		/**
		 * Add packages to the list of trusted packages used when constructing objects
		 * from JSON.
		 * <p>
		 * <strong>NOTE:</strong>If a class for a non-trusted package is encountered, the
		 * header is returned to the application with value of type
		 * {@link NonTrustedHeaderType}.
		 * @param packages the packages to include in the trusted list - if any entry is
		 * {@code "*"} all packages are trusted
		 * @return current builder
		 */
		public JsonPulsarHeaderMapperBuilder trustedPackages(String... packages) {
			this.trustedPackages.addAll(List.of(packages));
			return this;
		}

		/**
		 * Add class names to the list of classes that should be serialized using their
		 * {@link #toString()} method.
		 * @param classNames the class names to add to the 'toString' list
		 * @return current builder
		 */
		public JsonPulsarHeaderMapperBuilder toStringClasses(String... classNames) {
			this.toStringClasses.addAll(List.of(classNames));
			return this;
		}

		/**
		 * Adds to the list of patterns to be used for inbound header matching.
		 * @param patterns inbound patterns to add
		 * @return current builder
		 */
		public JsonPulsarHeaderMapperBuilder inboundPatterns(String... patterns) {
			this.inboundPatterns.addAll(List.of(patterns));
			return this;
		}

		/**
		 * Adds to the list of patterns to be used for outbound header matching.
		 * @param patterns outbound patterns to add
		 * @return current builder
		 */
		public JsonPulsarHeaderMapperBuilder outboundPatterns(String... patterns) {
			this.outboundPatterns.addAll(List.of(patterns));
			return this;
		}

		public JsonPulsarHeaderMapper build() {
			if (this.objectMapper == null) {
				this.objectMapper = JacksonUtils.enhancedObjectMapper();
			}
			return new JsonPulsarHeaderMapper(this.objectMapper, this.inboundPatterns, this.outboundPatterns,
					this.trustedPackages, this.toStringClasses);
		}

	}

}
