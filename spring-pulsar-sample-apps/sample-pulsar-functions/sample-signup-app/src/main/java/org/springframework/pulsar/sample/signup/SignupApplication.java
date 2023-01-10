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

package org.springframework.pulsar.sample.signup;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.sample.signup.model.Customer;
import org.springframework.pulsar.sample.signup.model.Signup;
import org.springframework.pulsar.sample.signup.model.SignupGenerator;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@SpringBootApplication
@EnableScheduling
public class SignupApplication {

	private final Logger logger = LoggerFactory.getLogger(SignupApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(SignupApplication.class, args);
	}

	@Autowired
	private CassandraTemplate cassandra;

	@Autowired
	private RabbitTemplate rabbit;

	@Autowired
	private SignupGenerator signupGenerator;

	@Scheduled(initialDelay = 5_000, fixedDelay = 5_000)
	void produceSignupToRabbit() {
		Signup signup = this.signupGenerator.generate();
		this.rabbit.convertAndSend("user_signup", signup);
		this.logger.info("TO RABBIT user_signup => {}", signup);
	}

	@PulsarListener(topics = "user-signup", schemaType = SchemaType.JSON, subscriptionName = "pl-us-sub",
			subscriptionType = SubscriptionType.Shared)
	void logUserSignups(Signup signup) {
		this.logger.info("FROM PULSAR user-signup => {}", signup);
	}

	@PulsarListener(topics = "customer-onboard", schemaType = SchemaType.JSON, subscriptionName = "pl-co-sub",
			subscriptionType = SubscriptionType.Shared)
	void logCustomerOnboards(Customer customer) {
		this.logger.info("FROM PULSAR customer-onboard => {}", customer);
	}

	@Scheduled(initialDelay = 10_000, fixedDelay = 15_000)
	void logSinkedOnboardInCassandra() {
		List<String> emails = this.cassandra.getCqlOperations()
				.queryForList("SELECT customer_email FROM customer_onboard", String.class);
		Collections.reverse(emails);
		String lastFiveEmails = emails.stream().limit(5).collect(Collectors.joining(", "));
		this.logger.info("FROM CASSANDRA => latest (5/{}) emails: {}...", emails.size(), lastFiveEmails);
	}

}
