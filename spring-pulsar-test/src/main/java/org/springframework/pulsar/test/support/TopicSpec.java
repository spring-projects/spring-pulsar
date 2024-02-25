/*
 * Copyright 2024 the original author or authors.
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

package org.springframework.pulsar.test.support;

/**
 * Specification for defining the topic to consume from for the
 * {@link PulsarConsumerTestUtil}.
 *
 * @author Jonas Geiregat
 */
public interface TopicSpec<T> {

	/**
	 * Define the topic to consume from.
	 * @param topic the topic to consume from
	 * @return the schema selection specification
	 */
	SchemaSpec<T> fromTopic(String topic);

}
