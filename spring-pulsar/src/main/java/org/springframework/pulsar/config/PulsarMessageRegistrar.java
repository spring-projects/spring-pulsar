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

package org.springframework.pulsar.config;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.pulsar.annotation.PulsarMessage;
import org.springframework.pulsar.core.DefaultSchemaResolver;
import org.springframework.pulsar.core.DefaultTopicResolver;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Registers default schema and topic mappings based on {@code descriptors} property
 *
 * @author Aleksei Arsenev
 */
public class PulsarMessageRegistrar implements InitializingBean {

	private List<MessageDescriptor> descriptors = new ArrayList<>();

	@Autowired
	private DefaultTopicResolver defaultTopicResolver = null;

	@Autowired
	private DefaultSchemaResolver defaultSchemaResolver = null;

	public List<MessageDescriptor> getDescriptors() {
		return descriptors;
	}

	public void setDescriptors(List<MessageDescriptor> descriptors) {
		this.descriptors = descriptors;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		for (MessageDescriptor descriptor : this.descriptors) {
			if (this.defaultTopicResolver != null && StringUtils.hasText(descriptor.topic)) {
				this.defaultTopicResolver.addCustomTopicMapping(descriptor.message, descriptor.topic);
			}

			if (this.defaultSchemaResolver != null && descriptor.schemaType != SchemaType.NONE) {
				Schema<Object> schema = this.defaultSchemaResolver
						.resolveSchema(descriptor.schemaType, descriptor.message, descriptor.messageKeyType)
						.orElseThrow();
				this.defaultSchemaResolver.addCustomSchemaMapping(descriptor.message, schema);
			}
		}
	}

	public static final class MessageDescriptor {

		private final Class<?> message;

		private final String topic;

		private final SchemaType schemaType;

		private final Class<?> messageKeyType;

		public MessageDescriptor(Class<?> message, MergedAnnotation<PulsarMessage> annotation) {
			this.message = message;
			this.topic = annotation.getString("topic");
			this.schemaType = annotation.getEnum("type", SchemaType.class);

			Class<?> messageKeyType = annotation.getClass("messageKeyType");
			if (messageKeyType == Void.class) {
				messageKeyType = null;
			}
			this.messageKeyType = messageKeyType;
		}

	}

}
