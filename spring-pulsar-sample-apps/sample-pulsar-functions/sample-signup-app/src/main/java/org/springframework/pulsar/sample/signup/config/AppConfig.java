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

package org.springframework.pulsar.sample.signup.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.io.SourceConfig;

import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.function.PulsarFunction;
import org.springframework.pulsar.function.PulsarFunctionOperations.FunctionStopPolicy;
import org.springframework.pulsar.function.PulsarSink;
import org.springframework.pulsar.function.PulsarSource;
import org.springframework.pulsar.sample.signup.model.SignupGenerator;

@Configuration(proxyBeanMethods = false)
class AppConfig {

	@Bean
	SignupGenerator signupGenerator() {
		return new SignupGenerator();
	}

	@Bean
	Jackson2JsonMessageConverter jackson2JsonMessageConverter() {
		return new Jackson2JsonMessageConverter();
	}

	@Bean
	PulsarSource userSignupRabbitSource() {
		Map<String, Object> configs = new HashMap<>();
		configs.put("host", "rabbitmq");
		configs.put("port", 5672);
		configs.put("virtualHost", "/");
		configs.put("username", "guest");
		configs.put("password", "guest");
		configs.put("queueName", "user_signup");
		configs.put("connectionName", "user_signup_pulsar_source");
		SourceConfig sourceConfig = SourceConfig.builder().tenant("public").namespace("default")
				.name("UserSignupRabbitSource").archive("builtin://rabbitmq").topicName("user-signup").configs(configs)
				.build();
		return new PulsarSource(sourceConfig, FunctionStopPolicy.DELETE, null);
	}

	@Bean
	PulsarFunction userSignupFunction(@Value("${PWD:.}") String currentRunDir) {
		// Abs path differs when run from w/in IDE and on command line (figure it out)
		String repoRelativePathToFunctionJar = "/spring-pulsar/spring-pulsar-sample-apps"
				+ "/sample-pulsar-functions/sample-signup-function"
				+ "/build/libs/sample-signup-function-0.1.1-SNAPSHOT.jar";
		int idx = currentRunDir.indexOf("/spring-pulsar");
		String absPathToRepo = currentRunDir.substring(0, Math.max(0, idx));
		String absPathToFunctionJar = absPathToRepo + repoRelativePathToFunctionJar;
		FunctionConfig functionConfig = FunctionConfig.builder().tenant("public").namespace("default")
				.name("UserSignupFunction").className("org.springframework.pulsar.sample.signup.SignupFunction")
				.jar(absPathToFunctionJar).inputs(List.of("user-signup")).build();
		return new PulsarFunction(functionConfig, FunctionStopPolicy.DELETE, null);
	}

	@Bean
	PulsarSink customerOnboardCassandraSink() {
		Map<String, Object> configs = new HashMap<>();
		configs.put("roots", "cassandra:9042");
		configs.put("keyspace", "sample_pulsar_functions_keyspace");
		configs.put("columnFamily", "customer_onboard");
		configs.put("keyname", "customer_email");
		configs.put("columnName", "customer_details");
		SinkConfig sinkConfig = SinkConfig.builder().tenant("public").namespace("default")
				.name("CustomerOnboardCassandraSink").archive("builtin://cassandra").inputs(List.of("customer-onboard"))
				.configs(configs).build();
		return new PulsarSink(sinkConfig, FunctionStopPolicy.DELETE, null);
	}

}
