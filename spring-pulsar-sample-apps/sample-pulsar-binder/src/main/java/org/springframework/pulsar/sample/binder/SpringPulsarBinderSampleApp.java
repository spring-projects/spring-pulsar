/*
 * Copyright 2022-2023 the original author or authors.
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

package org.springframework.pulsar.sample.binder;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class SpringPulsarBinderSampleApp {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public static void main(String[] args) {
		SpringApplication.run(SpringPulsarBinderSampleApp.class, args);
	}

	@Bean
	public Supplier<Time> timeSupplier() {
		return () -> new Time(String.valueOf(System.currentTimeMillis()));
	}

	@Bean
	public Function<Time, EnhancedTime> timeProcessor() {
		return (time) -> {
			EnhancedTime enhancedTime = new EnhancedTime(time, "5150");
			this.logger.info("PROCESSOR: {} --> {}", time, enhancedTime);
			return enhancedTime;
		};
	}

	@Bean
	public Consumer<EnhancedTime> timeLogger() {
		return (time) -> this.logger.info("SINK:      {}", time);
	}

	record Time(String time) {
	}

	record EnhancedTime(Time time, String extra) {
	}

}
