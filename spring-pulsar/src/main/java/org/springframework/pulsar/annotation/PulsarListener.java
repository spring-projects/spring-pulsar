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

package org.springframework.pulsar.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.pulsar.config.PulsarListenerContainerFactory;
import org.springframework.pulsar.config.PulsarListenerEndpointRegistry;
import org.springframework.pulsar.listener.AckMode;

/**
 * Annotation that marks a method to be the target of a Pulsar message listener on the
 * specified topics.
 *
 * The {@link #containerFactory()} identifies the {@link PulsarListenerContainerFactory}
 * to use to build the Pulsar listener container. If not set, a <em>default</em> container
 * factory is assumed to be available with a bean name of
 * {@code pulsarListenerContainerFactory} unless an explicit default has been provided
 * through configuration.
 *
 * <p>
 * Processing of {@code @PulsarListener} annotations is performed by registering a
 * {@link PulsarListenerAnnotationBeanPostProcessor}. This can be done manually or, more
 * conveniently, through {@link EnablePulsar} annotation.
 * </p>
 *
 * @author Soby Chacko
 * @author Chris Bono
 * @author Alexander Preuß
 */
@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@MessageMapping
@Documented
public @interface PulsarListener {

	/**
	 * The unique identifier of the container for this listener.
	 * <p>
	 * If none is specified an auto-generated id is used.
	 * <p>
	 * SpEL {@code #{...}} and property place holders {@code ${...}} are supported.
	 * @return the {@code id} for the container managing for this endpoint.
	 * @see PulsarListenerEndpointRegistry#getListenerContainer(String)
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
	 * Set to true or false, to override the default setting in the container factory. May
	 * be a property placeholder or SpEL expression that evaluates to a {@link Boolean} or
	 * a {@link String}, in which case the {@link Boolean#parseBoolean(String)} is used to
	 * obtain the value.
	 * <p>
	 * SpEL {@code #{...}} and property placeholders {@code ${...}} are supported.
	 * @return true to auto start, false to not auto start.
	 */
	String autoStartup() default "";

	/**
	 * Activate batch consumption.
	 * @return whether this listener is in batch mode or not.
	 */
	boolean batch() default false;

	/**
	 * A pseudo bean name used in SpEL expressions within this annotation to reference the
	 * current bean within which this listener is defined. This allows access to
	 * properties and methods within the enclosing bean. Default '__listener'.
	 * <p>
	 * @return the pseudo bean name.
	 */
	String beanRef() default "__listener";

	/**
	 * Pulsar consumer properties; they will supersede any properties with the same name
	 * defined in the consumer factory (if the consumer factory supports property
	 * overrides).
	 * <p>
	 * <b>Supported Syntax</b>
	 * <p>
	 * The supported syntax for key-value pairs is the same as the syntax defined for
	 * entries in a Java {@linkplain java.util.Properties#load(java.io.Reader) properties
	 * file}:
	 * <ul>
	 * <li>{@code key=value}</li>
	 * <li>{@code key:value}</li>
	 * <li>{@code key value}</li>
	 * </ul>
	 * {@code group.id} and {@code client.id} are ignored.
	 * <p>
	 * SpEL {@code #{...}} and property place holders {@code ${...}} are supported. SpEL
	 * expressions must resolve to a {@link String}, a @{link String[]} or a
	 * {@code Collection<String>} where each member of the array or collection is a
	 * property name + value with the above formats.
	 * @return the properties.
	 */
	String[] properties() default {};

	/**
	 * Override the container factory's {@code concurrency} setting for this listener. May
	 * be a property placeholder or SpEL expression that evaluates to a {@link Number}, in
	 * which case {@link Number#intValue()} is used to obtain the value.
	 * <p>
	 * SpEL {@code #{...}} and property placeholders {@code ${...}} are supported.
	 * @return the concurrency.
	 */
	String concurrency() default "";

	/**
	 * The bean name or a 'SpEL' expression that resolves to a
	 * {@link org.apache.pulsar.client.api.RedeliveryBackoff} to use on the consumer to
	 * control the redelivery backoff of messages after a negative ack.
	 * @return the bean name or empty string to not set the backoff.
	 */
	String negativeAckRedeliveryBackoff() default "";

	/**
	 * The bean name or a 'SpEL' expression that resolves to a
	 * {@link org.apache.pulsar.client.api.RedeliveryBackoff} to use on the consumer to
	 * control the redelivery backoff of messages after an acknowledgment timeout.
	 * @return the bean name or empty string to not set the backoff.
	 */
	String ackTimeoutRedeliveryBackoff() default "";

	/**
	 * The bean name or a 'SpEL' expression that resolves to a
	 * {@link org.apache.pulsar.client.api.DeadLetterPolicy} to use on the consumer to
	 * configure a dead letter policy for message redelivery.
	 * @return the bean name or empty string to not set any dead letter policy.
	 */
	String deadLetterPolicy() default "";

	/**
	 * Override the container default ack mode of BATCH.
	 * @return ack mode used by the listener
	 */
	AckMode ackMode() default AckMode.BATCH;

	/**
	 * The bean name or a 'SpEL' expression that resolves to a
	 * {@link org.springframework.pulsar.listener.PulsarConsumerErrorHandler} which is
	 * used as a Spring provided mechanism to handle errors from processing the message.
	 * @return the bean name for the consumer error handler or an empty string.
	 */
	String pulsarConsumerErrorHandler() default "";

	/**
	 * The bean name or a 'SpEL' expression that resolves to a
	 * {@link PulsarListenerConsumerBuilderCustomizer} to use to configure the underlying
	 * consumer.
	 * @return the bean name or SpEL expression to the customizer or an empty string to
	 * not customize the consumer
	 */
	String consumerCustomizer() default "";

}
