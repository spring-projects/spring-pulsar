/*
 * Copyright 2022-2024 the original author or authors.
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

package com.example;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class SpringPulsarBinderSampleApp {

	private static final Logger LOG = LoggerFactory.getLogger(SpringPulsarBinderSampleApp.class);

	public static void main(String[] args) {
		SpringApplication.run(SpringPulsarBinderSampleApp.class, args);
	}

	private AtomicInteger counter = new AtomicInteger();

	@Bean
	ApplicationRunner fooSupplier(StreamBridge streamBridge) {
		return (args) -> {
			for (int i = 0; i < 10; i++) {
				var foo = new Foo("fooSupplier:" + i);
				streamBridge.send("fooSupplier-out-0", foo);
				LOG.info("++++++SOURCE {}------", foo);
			}
		};
	}

	@Bean
	public Function<Foo, Bar> fooProcessor() {
		return (foo) -> {
			var bar = new Bar(foo);
			LOG.info("++++++PROCESSOR {} --> {}------", foo, bar);
			return bar;
		};
	}

	@Bean
	public Consumer<Bar> barLogger() {
		return (bar) -> LOG.info("++++++SINK {}------", bar);
	}

	record Foo(String value) {
	}

	record Bar(Foo value) {
	}

}
