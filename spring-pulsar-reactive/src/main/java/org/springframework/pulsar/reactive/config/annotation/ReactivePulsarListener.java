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

package org.springframework.pulsar.reactive.config.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.pulsar.config.PulsarListenerContainerFactory;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerContainerFactory;
import org.springframework.pulsar.reactive.config.ReactivePulsarListenerEndpointRegistry;

/**
 * Annotation that marks a method to be the target of a Pulsar message listener on the
 * specified topics.
 *
 * The {@link #containerFactory()} identifies the
 * {@link ReactivePulsarListenerContainerFactory} to use to build the Pulsar listener
 * container. If not set, a <em>default</em> container factory is assumed to be available
 * with a bean name of {@code pulsarListenerContainerFactory} unless an explicit default
 * has been provided through configuration.
 *
 * <p>
 * Processing of {@code @ReactivePulsarListener} annotations is performed by registering a
 * {@link ReactivePulsarListenerAnnotationBeanPostProcessor}. This can be done manually
 * or, more conveniently, through {@link EnableReactivePulsar} annotation.
 * </p>
 *
 * @author Christophe Bornet
 */
@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@MessageMapping
@Documented
public @interface ReactivePulsarListener {

	/**
	 * The unique identifier of the container for this listener.
	 * <p>
	 * If none is specified an auto-generated id is used.
	 * <p>
	 * SpEL {@code #{...}} and property placeholders {@code ${...}} are supported.
	 * @return the {@code id} for the container managing for this endpoint.
	 * @see ReactivePulsarListenerEndpointRegistry#getListenerContainer(String)
	 */
	String id() default "";

	/**
	 * Pulsar subscription name associated with this listener.
	 * @return the {@code subscriptionName} for this Pulsar listener endpoint.
	 */
	String subscriptionName() default "";

	/**
	 * Pulsar subscription type for this listener - expected to be a single element array
	 * with subscription type or empty array to indicate null type.
	 * @return single element array with the subscription type or empty array to indicate
	 * no type chosen by user
	 */
	SubscriptionType[] subscriptionType() default { SubscriptionType.Exclusive };

	/**
	 * Pulsar schema type for this listener.
	 * @return the {@code schemaType} for this listener
	 */
	SchemaType schemaType() default SchemaType.NONE;

	/**
	 * The bean name of the {@link PulsarListenerContainerFactory} to use to create the
	 * message listener container responsible to serve this endpoint.
	 * <p>
	 * If not specified, the default container factory is used, if any. If a SpEL
	 * expression is provided ({@code #{...}}), the expression can either evaluate to a
	 * container factory instance or a bean name.
	 * @return the container factory bean name.
	 */
	String containerFactory() default "";

	/**
	 * Topics to listen to.
	 * @return a comma separated list of topics to listen from.
	 */
	String[] topics() default {};

	/**
	 * Topic patten to listen to.
	 * @return topic pattern to listen to.
	 */
	String topicPattern() default "";

	/**
	 * Whether to automatically start the container for this listener.
	 * <p>
	 * The value can be a literal string representation of boolean (e.g. {@code 'true'})
	 * or a property placeholder {@code ${...}} that resolves to a literal. SpEL
	 * {@code #{...}} expressions that evaluate to a {@link Boolean} or a literal are
	 * supported.
	 * @return whether to automatically start the container for this listener
	 */
	String autoStartup() default "";

	/**
	 * Activate stream consumption.
	 * @return if true, the listener method shall take a
	 * {@link reactor.core.publisher.Flux} as input argument.
	 */
	boolean stream() default false;

	/**
	 * A pseudo bean name used in SpEL expressions within this annotation to reference the
	 * current bean within which this listener is defined. This allows access to
	 * properties and methods within the enclosing bean. Default '__listener'.
	 * <p>
	 * @return the pseudo bean name.
	 */
	String beanRef() default "__listener";

	/**
	 * Override the container factory's {@code concurrency} setting for this listener.
	 * <p>
	 * The value can be a literal string representation of {@link Number} (e.g.
	 * {@code '3'}) or a property placeholder {@code ${...}} that resolves to a literal.
	 * SpEL {@code #{...}} expressions that evaluate to a {@link Number} or a literal are
	 * supported.
	 * @return the concurrency for this listener
	 */
	String concurrency() default "";

	/**
	 * Set to true or false, to override the default setting in the container factory. May
	 * be a property placeholder or SpEL expression that evaluates to a {@link Boolean} or
	 * a {@link String}, in which case the {@link Boolean#parseBoolean(String)} is used to
	 * obtain the value.
	 * <p>
	 * SpEL {@code #{...}} and property place holders {@code ${...}} are supported.
	 * @return true to keep ordering by message key when concurrency > 1, false to not
	 * keep ordering.
	 */
	String useKeyOrderedProcessing() default "";

	/**
	 * The bean name or a SpEL expression that resolves to a
	 * {@link org.apache.pulsar.client.api.DeadLetterPolicy} to use on the consumer to
	 * configure a dead letter policy for message redelivery.
	 * @return the bean name or empty string to not set any dead letter policy.
	 */
	String deadLetterPolicy() default "";

	/**
	 * The bean name or a SpEL expression that resolves to a
	 * {@link ReactivePulsarListenerMessageConsumerBuilderCustomizer} to use to configure
	 * the underlying consumer.
	 * @return the bean name or SpEL expression to the customizer or an empty string to
	 * not customize the consumer
	 */
	String consumerCustomizer() default "";

}
