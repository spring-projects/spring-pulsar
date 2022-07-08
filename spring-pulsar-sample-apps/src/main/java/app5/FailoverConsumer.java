/*
 * Copyright 2022 the original author or authors.
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

package app5;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.pulsar.annotation.PulsarListener;

@SpringBootApplication
public class FailoverConsumer {


	public static void main(String[] args) {
		String[] args1 = new String[]{
//				"--spring.pulsar.consumer.subscription-type=Failover",
				"--spring.pulsar.producer.messageRoutingMode=CustomPartition"};
		SpringApplication.run(FailoverConsumer.class, args1);
	}


	@PulsarListener(subscriptionName = "failover-subscription-demo",  topics = "failover-demo-topic", subscriptionType = "shared")
	public void listen1(String foo) {
		//...
	}


}

