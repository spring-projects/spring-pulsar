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

import org.apache.pulsar.common.schema.SchemaType;

/**
 * Specifies default topic and schema info for a message class.
 * <p>
 * When a message class is marked with this annotation, the topic/schema resolution
 * process will use the specified information to determine a topic/schema to use for the
 * message in process.
 *
 * @author Aleksei Arsenev
 * @author Chris Bono
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface PulsarMessage {

	/**
	 * Default topic for the annotated message class.
	 * @return default topic for the annotated message class or empty string to indicate
	 * no default topic is specified
	 */
	String topic() default "";

	/**
	 * Default schema type to use for the annotated message class.
	 * <p>
	 * Note that when this is set to {@code KEY_VALUE} you must specify the actual key and
	 * value information via the {@link #messageKeyType()} and
	 * {@link #messageValueSchemaType()} attributes, respectively.
	 * @return schema type to use for the annotated message class or {@code NONE} to
	 * indicate no default schema is specified
	 */
	SchemaType schemaType() default SchemaType.NONE;

	/**
	 * The message key type when schema type is set to {@code KEY_VALUE}.
	 * <p>
	 * When the {@link #schemaType()} is not set to {@code KEY_VALUE} this attribute is
	 * ignored.
	 * @return message key type when using {@code KEY_VALUE} schema type
	 */
	Class<?> messageKeyType() default Void.class;

	/**
	 * The default schema type to use for the value schema when {@link #schemaType()} is
	 * set to {@code KEY_VALUE}.
	 * <p>
	 * When the {@link #schemaType()} is not set to {@code KEY_VALUE} this attribute is
	 * ignored and the default schema type must be specified via the {@code schemaType}
	 * attribute.
	 * @return message value schema type when using {@code KEY_VALUE} schema type
	 */
	SchemaType messageValueSchemaType() default SchemaType.NONE;

}
