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

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.core.PulsarTopic;

@SpringBootConfiguration
@EnableAutoConfiguration
@Profile("smoketest.pulsar.imperative")
class ImperativeAppConfig {

	private static final Log LOG = LogFactory.getLog(ImperativeAppConfig.class);

	private static final String TOPIC = "pulsar-inttest-topic";

	@Bean
	PulsarTopic pulsarTestTopic() {
		return PulsarTopic.builder(TOPIC).numberOfPartitions(1).build();
	}

	@Bean
	ApplicationRunner sendMessagesToPulsarTopic(PulsarTemplate<SampleMessage> template) {
		return (args) -> {
			for (int i = 0; i < 10; i++) {
				template.send(TOPIC, new SampleMessage(i, "message:" + i));
				LOG.info("++++++PRODUCE IMPERATIVE:(" + i + ")------");
			}
		};
	}

	@PulsarListener(topics = TOPIC)
	void consumeMessagesFromPulsarTopic(SampleMessage msg) {
		LOG.info("++++++CONSUME IMPERATIVE:(" + msg.id() + ")------");
	}

}
