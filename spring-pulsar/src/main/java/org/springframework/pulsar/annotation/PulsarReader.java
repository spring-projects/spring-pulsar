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

package org.springframework.pulsar.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.pulsar.config.PulsarReaderEndpointRegistry;

@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@MessageMapping
@Documented
public @interface PulsarReader {

	/**
	 * The unique identifier of the container for this reader.
	 * <p>
	 * If none is specified an auto-generated id is used.
	 * <p>
	 * SpEL {@code #{...}} and property placeholders {@code ${...}} are supported.
	 * @return the id of the container for this reader
	 * @see PulsarReaderEndpointRegistry#getReaderContainer(String)
	 */
	String id() default "";

	/**
	 * Pulsar schema type for this listener.
	 * @return the {@code schemaType} for this listener
	 */
	SchemaType schemaType() default SchemaType.NONE;

	/**
	 * {@link MessageId} for this reader to start from.
	 * @return starting message id - earliest or latest.
	 */
	String startMessageId() default "";

	/**
	 * Topics to listen to.
	 * @return a comma separated list of topics to listen from.
	 */
	String[] topics() default {};

	String beanRef() default "__listener";

	String containerFactory() default "";

	String autoStartup() default "";

	/**
	 * The bean name or a SpEL expression that resolves to a
	 * {@link PulsarReaderReaderBuilderCustomizer} to use to configure the reader.
	 * @return the bean name or empty string to not configure the reader.
	 */
	String readerCustomizer() default "";

}
