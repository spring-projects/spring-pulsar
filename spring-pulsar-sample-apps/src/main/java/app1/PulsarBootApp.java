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
		pulsarTemplate.setDefaultTopicName("hello-pulsar-exclusive-2");
		return args -> {
//			for (int i = 0; i < 100; i ++) {
//				pulsarTemplate.send("This is message " + (i + 1));
//			}
			Foo foo = new Foo();
			foo.setFoo("Foo");
			foo.setBar("Bar");
			pulsarTemplate.send(foo);

		};
	}

	@PulsarListener(subscriptionName = "test-exclusive-sub-2", topics = "hello-pulsar-exclusive-2", schemaType = SchemaType.JSON)
	public void listen(Foo foo) {
		System.out.println("Message received: " + foo);
	}

	static class Foo {
		String foo;
		String bar;

		public String getFoo() {
			return foo;
		}

		public void setFoo(String foo) {
			this.foo = foo;
		}

		public String getBar() {
			return bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

		@Override
		public String toString() {
			return "Foo{" +
					"foo='" + foo + '\'' +
					", bar='" + bar + '\'' +
					'}';
		}
	}


}
