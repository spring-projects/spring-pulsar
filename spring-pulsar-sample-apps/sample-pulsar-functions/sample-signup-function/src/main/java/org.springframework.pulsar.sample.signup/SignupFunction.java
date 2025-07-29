/*
 * Copyright 2023-present the original author or authors.
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

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import org.springframework.pulsar.sample.signup.model.Customer;
import org.springframework.pulsar.sample.signup.model.Signup;
import org.springframework.pulsar.sample.signup.model.SignupTier;

public class SignupFunction implements Function<Signup, Void> {

	@Override
	public Void process(Signup signup, Context context) {
		log("Processing " + signup);

		// Count and log the signups for tier
		SignupTier tier = signup.getSignupTier();
		context.incrCounter(tier.name(), 1L);
		long count = context.getCounter(tier.name());
		log(String.format("   %s signup count: %d", tier.name(), count));

		// Create customer onboard for enterprise signups
		if (tier == SignupTier.ENTERPRISE) {
			Customer customer = Customer.from(signup);
			log("Converting to " + signup);
			try {
				context.newOutputMessage("customer-onboard", Schema.JSON(Customer.class))
						.key(customer.getEmail())
						.value(customer)
						.eventTime(System.currentTimeMillis())
						.send();
			} catch (PulsarClientException e) {
				e.printStackTrace();
				throw new RuntimeException(e.getMessage(), e);
			}
		}
		return null;
	}

	private void log(String msg) {
		// Typically logging is done via context.getLogger() but that logs to a topic
		// which is not helpful during dev/debugging - sending to console for that reason
		System.out.println(msg);
	}
}
