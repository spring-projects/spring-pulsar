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

import org.apache.pulsar.common.schema.SchemaType;

import java.lang.annotation.*;

/**
 * Specifies default topic and schema for class.
 *
 * @author Aleksei Arsenev
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface PulsarTypeMapping {

	/**
	 * Default topic for class.
	 * @return topic
	 */
	String topic() default "";

	/**
	 * Default schema type for class.
	 * @return schema type
	 */
	SchemaType schemaType() default SchemaType.NONE;

	/**
	 * Message key type (must be specified when schema type is {@code KEY_VALUE})
	 * @return message key type
	 */
	Class<?> messageKeyType() default Void.class;

}
