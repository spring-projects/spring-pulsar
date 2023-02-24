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

package reader.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.pulsar.annotation.PulsarReader;
import org.springframework.pulsar.core.PulsarTemplate;

@SpringBootApplication
public class SpringPulsarReaderBootApp {

	private static final Logger logger = LoggerFactory.getLogger(SpringPulsarReaderBootApp.class);

	public static void main(String[] args) {
		SpringApplication.run(SpringPulsarReaderBootApp.class, args);
	}

	/*
	 * Basic publisher using PulsarTemplate<String> and a PulsarReader.
	 */
	@Bean
	ApplicationRunner runner1(PulsarTemplate<String> pulsarTemplate) {

		String topic1 = "pulsar-reader-demo-topic";

		return args -> {
			for (int i = 0; i < 10; i++) {
				pulsarTemplate.send(topic1, "This is message " + (i + 1));
			}
		};
	}

	@PulsarReader(id = "my-id", subscriptionName = "pulsar-reader-demo-subscription",
			topics = "pulsar-reader-demo-topic", startMessageId = "earliest")
	void read(String message) {
		logger.info(message);
	}

}
