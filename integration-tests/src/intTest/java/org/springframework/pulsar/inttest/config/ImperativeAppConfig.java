/*
 * Copyright 2023-2024 the original author or authors.
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

package org.springframework.pulsar.inttest.config;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.core.DefaultPulsarConsumerFactory;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.core.PulsarConsumerFactory;
import org.springframework.pulsar.core.PulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.PulsarTopicBuilder;
import org.springframework.pulsar.core.TopicResolver;

@SpringBootConfiguration
@EnableAutoConfiguration
@Profile("inttest.pulsar.imperative")
class ImperativeAppConfig {

	private static final Log LOG = LogFactory.getLog(ImperativeAppConfig.class);
	static final String TENANT = "my-tenant-i";
	static final String NAMESPACE = "my-namespace-i";
	static final String NFQ_TOPIC = "dtant-topic-i";
	static final String FQ_TOPIC = "persistent://my-tenant-i/my-namespace-i/dtant-topic-i";
	static final String MSG_PREFIX = "DefaultTenantNamespace-i:";

	@Bean
	PulsarProducerFactory<Object> pulsarProducerFactory(PulsarClient pulsarClient, TopicResolver topicResolver,
			PulsarTopicBuilder topicBuilder) {
		var producerFactory = new DefaultPulsarProducerFactory<>(pulsarClient, null, null, topicResolver);
		producerFactory.setTopicBuilder(topicBuilder);
		return producerFactory;
	}

	@Bean
	PulsarConsumerFactory<Object> pulsarConsumerFactory(PulsarClient pulsarClient, PulsarTopicBuilder topicBuilder) {
		var consumerFactory = new DefaultPulsarConsumerFactory<>(pulsarClient, null);
		consumerFactory.setTopicBuilder(topicBuilder);
		return consumerFactory;
	}

	@PulsarListener(topics = NFQ_TOPIC)
	void consumeFromNonFullyQualifiedTopic(String msg) {
		LOG.info("++++++CONSUME %s------".formatted(msg));
	}

	@PulsarListener(topics = FQ_TOPIC)
	void consumeFromFullyQualifiedTopic(String msg) {
		LOG.info("++++++CONSUME %s------".formatted(msg));
	}

	@Bean
	ApplicationRunner produceWithDefaultTenantAndNamespace(PulsarAdministration pulsarAdmin,
			PulsarTemplate<String> template) {
		createTenantAndNamespace(pulsarAdmin);
		return (args) -> {
			for (int i = 0; i < 10; i++) {
				var msg = MSG_PREFIX + i;
				template.send((i < 5) ? FQ_TOPIC : NFQ_TOPIC, msg);
				LOG.info("++++++PRODUCE %s------".formatted(msg));
			}
		};
	}

	private void createTenantAndNamespace(PulsarAdministration pulsarAdmin) {
		try (var admin = pulsarAdmin.createAdminClient()) {
			admin.tenants()
				.createTenant(TENANT, TenantInfoImpl.builder().allowedClusters(Set.of("standalone")).build());
			LOG.info("Created tenant -> %s".formatted(admin.tenants().getTenantInfo(TENANT)));
			admin.namespaces().createNamespace("%s/%s".formatted(TENANT, NAMESPACE));
			LOG.info("Created namespace -> %s".formatted(admin.namespaces().getNamespaces(TENANT)));
		}
		catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

}
