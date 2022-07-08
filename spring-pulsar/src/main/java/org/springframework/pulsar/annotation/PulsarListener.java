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

package org.springframework.pulsar.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.messaging.handler.annotation.MessageMapping;

/**
 * Annotation that marks a method to be the target of a Pulsar message listener on the
 * specified topics.
 *
 * The {@link #containerFactory()} identifies the
 * {@link org.springframework.pulsar.config.PulsarListenerContainerFactory} to use to build the Pulsar listener container.
 * If not set, a <em>default</em> container factory is assumed to be available with a bean name
 * of {@code pulsarListenerContainerFactory} unless an explicit default has been provided
 * through configuration.
 *
 * <p>
 * Processing of {@code @PulsarListener} annotations is performed by registering a
 * {@link PulsarListenerAnnotationBeanPostProcessor}. This can be done manually or, more
 * conveniently, through {@link EnablePulsar} annotation.
 * </p>
 *
 * @author Soby Chacko
 */
@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@MessageMapping
@Documented
public @interface PulsarListener {

	/**
	 * The unique identifier of the container for this listener.
	 * <p>If none is specified an auto-generated id is used.
	 * <p>SpEL {@code #{...}} and property place holders {@code ${...}} are supported.
	 * @return the {@code id} for the container managing for this endpoint.
	 * @see org.springframework.pulsar.config.PulsarListenerEndpointRegistry#getListenerContainer(String)
	 */
	String id() default "";

	/**
	 * Pulsar subscription name associated with this listener.
	 * @return the {@code subscriptionName} for this Pulsar listener endpoint.
	 */
	String subscriptionName() default "";

	/**
	 * Pulsar subscription type for this listener.
	 * @return the {@code subscriptionType} for this listener
	 */
	String subscriptionType() default "";

	SchemaType schemaType() default SchemaType.NONE;

	/**
	 * Specific container factory to use on this listener.
	 * @return {@code containerFactory} to use on this Pulsar listener.
	 */
	String containerFactory() default "";

	/**
	 * Topics to listen to.
	 *
	 * @return a comma separated list of topics to listen from.
	 */
	String[] topics() default {};

	/**
	 * Topic patten to listen to.
	 *
	 * @return topic pattern to listen to.
	 */
	String topicPattern() default "";

	/**
	 * Set to true or false, to override the default setting in the container factory. May
	 * be a property placeholder or SpEL expression that evaluates to a {@link Boolean} or
	 * a {@link String}, in which case the {@link Boolean#parseBoolean(String)} is used to
	 * obtain the value.
	 * <p>SpEL {@code #{...}} and property place holders {@code ${...}} are supported.
	 * @return true to auto start, false to not auto start.
	 */
	String autoStartup() default "";

	/**
	 * Activate batch consumption.
	 *
	 * @return whether this listener is in batch mode or not.
	 */
	String batch() default "";

	/**
	 * A pseudo bean name used in SpEL expressions within this annotation to reference
	 * the current bean within which this listener is defined. This allows access to
	 * properties and methods within the enclosing bean.
	 * Default '__listener'.
	 * <p>
	 * @return the pseudo bean name.
	 */
	String beanRef() default "__listener";

	String[] properties() default {};
}
