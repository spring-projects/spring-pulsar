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

import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.reactive.client.api.ReactivePulsarClient;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.core.PulsarTopicBuilder;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListener;
import org.springframework.pulsar.reactive.core.DefaultReactivePulsarConsumerFactory;
import org.springframework.pulsar.reactive.core.DefaultReactivePulsarSenderFactory;
import org.springframework.pulsar.reactive.core.ReactivePulsarConsumerFactory;
import org.springframework.pulsar.reactive.core.ReactivePulsarSenderFactory;
import org.springframework.pulsar.reactive.core.ReactivePulsarTemplate;

import reactor.core.publisher.Mono;

@SpringBootConfiguration
@EnableAutoConfiguration
@Profile("inttest.pulsar.reactive")
class ReactiveAppConfig {

	private static final Log LOG = LogFactory.getLog(ReactiveAppConfig.class);
	static final String TENANT = "my-tenant-r";
	static final String NAMESPACE = "my-namespace-r";
	static final String NFQ_TOPIC = "dtant-topic-r";
	static final String FQ_TOPIC = "persistent://my-tenant-r/my-namespace-r/dtant-topic-r";
	static final String MSG_PREFIX = "DefaultTenantNamespace-r:";

	@Bean
	ReactivePulsarSenderFactory<Object> reactivePulsarSenderFactory(ReactivePulsarClient reactivePulsarClient,
			PulsarTopicBuilder topicBuilder) {
		return DefaultReactivePulsarSenderFactory.builderFor(reactivePulsarClient)
			.withTopicBuilder(topicBuilder)
			.build();
	}

	@Bean
	ReactivePulsarConsumerFactory<Object> reactivePulsarConsumerFactory(ReactivePulsarClient reactivePulsarClient,
			PulsarTopicBuilder topicBuilder) {
		var consumerFactory = new DefaultReactivePulsarConsumerFactory<>(reactivePulsarClient, List.of());
		consumerFactory.setTopicBuilder(topicBuilder);
		return consumerFactory;
	}

	@ReactivePulsarListener(topics = NFQ_TOPIC)
	Mono<Void> consumeFromNonFullyQualifiedTopic(String msg) {
		LOG.info("++++++CONSUME %s------".formatted(msg));
		return Mono.empty();
	}

	@ReactivePulsarListener(topics = FQ_TOPIC)
	Mono<Void> consumeFromFullyQualifiedTopic(String msg) {
		LOG.info("++++++CONSUME %s------".formatted(msg));
		return Mono.empty();
	}

	@Bean
	ApplicationRunner produceWithDefaultTenantAndNamespace(PulsarAdministration pulsarAdmin,
			ReactivePulsarTemplate<String> template) {
		createTenantAndNamespace(pulsarAdmin);
		return (args) -> {
			for (int i = 0; i < 10; i++) {
				var msg = MSG_PREFIX + i;
				template.send((i < 5) ? FQ_TOPIC : NFQ_TOPIC, msg).subscribe();
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
