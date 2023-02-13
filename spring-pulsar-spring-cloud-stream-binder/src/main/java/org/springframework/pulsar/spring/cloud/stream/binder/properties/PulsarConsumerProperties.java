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

package org.springframework.pulsar.spring.cloud.stream.binder.properties;

import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.lang.Nullable;
import org.springframework.pulsar.autoconfigure.ConsumerConfigProperties;

/**
 * Pulsar consumer properties used by the binder.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public class PulsarConsumerProperties extends ConsumerConfigProperties {

	@Nullable
	private SchemaType schemaType;

	@Nullable
	private Class<?> messageType;

	@Nullable
	private Class<?> messageKeyType;

	@Nullable
	private Class<?> messageValueType;

	private int partitionCount = 0;

	@Nullable
	public SchemaType getSchemaType() {
		return this.schemaType;
	}

	public void setSchemaType(@Nullable SchemaType schemaType) {
		this.schemaType = schemaType;
	}

	@Nullable
	public Class<?> getMessageType() {
		return this.messageType;
	}

	public void setMessageType(@Nullable Class<?> messageType) {
		this.messageType = messageType;
	}

	@Nullable
	public Class<?> getMessageKeyType() {
		return this.messageKeyType;
	}

	public void setMessageKeyType(@Nullable Class<?> messageKeyType) {
		this.messageKeyType = messageKeyType;
	}

	@Nullable
	public Class<?> getMessageValueType() {
		return this.messageValueType;
	}

	public void setMessageValueType(@Nullable Class<?> messageValueType) {
		this.messageValueType = messageValueType;
	}

	public int getPartitionCount() {
		return this.partitionCount;
	}

	public void setPartitionCount(int partitionCount) {
		this.partitionCount = partitionCount;
	}

}
