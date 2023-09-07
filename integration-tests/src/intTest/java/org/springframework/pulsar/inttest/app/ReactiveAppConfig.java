/*
 * Copyright 2012-2023 the original author or authors.
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

package org.springframework.pulsar.inttest.app;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pulsar.reactive.client.api.MessageSpec;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.pulsar.core.PulsarTopic;
import org.springframework.pulsar.reactive.config.annotation.ReactivePulsarListener;
import org.springframework.pulsar.reactive.core.ReactivePulsarTemplate;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SpringBootConfiguration
@EnableAutoConfiguration
@Profile("smoketest.pulsar.reactive")
class ReactiveAppConfig {

	private static final Log LOG = LogFactory.getLog(ReactiveAppConfig.class);

	private static final String TOPIC = "pulsar-reactive-inttest-topic";

	@Bean
	PulsarTopic pulsarTestTopic() {
		return PulsarTopic.builder(TOPIC).numberOfPartitions(1).build();
	}

	@Bean
	ApplicationRunner sendMessagesToPulsarTopic(ReactivePulsarTemplate<SampleMessage> template) {
		return (args) -> Flux.range(0, 10)
			.map((i) -> new SampleMessage(i, "message:" + i))
			.map(MessageSpec::of)
			.as((msgs) -> template.send(TOPIC, msgs))
			.doOnNext((sendResult) -> LOG
				.info("++++++PRODUCE REACTIVE:(" + sendResult.getMessageSpec().getValue().id() + ")------"))
			.subscribe();
	}

	@ReactivePulsarListener(topics = TOPIC)
	Mono<Void> consumeMessagesFromPulsarTopic(SampleMessage msg) {
		LOG.info("++++++CONSUME REACTIVE:(" + msg.id() + ")------");
		return Mono.empty();
	}

}
