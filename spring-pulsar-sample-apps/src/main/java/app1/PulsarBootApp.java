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

package app1;

import org.apache.pulsar.common.schema.SchemaType;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.core.PulsarTemplate;

@SpringBootApplication
public class PulsarBootApp {

	public static void main(String[] args) {
		SpringApplication.run(PulsarBootApp.class, args);
	}

	@Bean
	public ApplicationRunner runner(PulsarTemplate<Foo> pulsarTemplate) {
		String topic = "hello-pulsar-exclusive-2";
		return args -> {
//			for (int i = 0; i < 100; i ++) {
//				pulsarTemplate.send(topic, "This is message " + (i + 1));
//			}
			Foo foo = new Foo();
			foo.setFoo("Foo");
			foo.setBar("Bar");
			pulsarTemplate.send(topic, foo);

		};
	}

	@PulsarListener(subscriptionName = "test-exclusive-sub-2", topics = "hello-pulsar-exclusive-2", schemaType = SchemaType.JSON)
	public void listen(Foo foo) {
		//...
	}

	static class Foo {
		String foo;
		String bar;

		public String getFoo() {
			return this.foo;
		}

		public void setFoo(String foo) {
			this.foo = foo;
		}

		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

		@Override
		public String toString() {
			return "Foo{" +
					"foo='" + this.foo + '\'' +
					", bar='" + this.bar + '\'' +
					'}';
		}
	}


}
