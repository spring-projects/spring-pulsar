/*
 * Copyright 2022-present the original author or authors.
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

package org.springframework.pulsar.listener;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.Test;

import org.springframework.context.annotation.Configuration;
import org.springframework.pulsar.annotation.EnablePulsar;
import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.pulsar.core.DefaultPulsarProducerFactory;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.pulsar.listener.PulsarListenerAutoConsumeSchemaTests.PulsarListenerAutoConsumeSchemaTestsConfig;
import org.springframework.pulsar.test.model.UserPojo;
import org.springframework.pulsar.test.model.UserRecord;
import org.springframework.test.context.ContextConfiguration;

/**
 * Tests for {@link PulsarListener @PulsarListener} using {@code schemaType} of
 * {@link SchemaType#AUTO_CONSUME}.
 *
 * @author Chris Bono
 */
@ContextConfiguration(classes = PulsarListenerAutoConsumeSchemaTestsConfig.class)
class PulsarListenerAutoConsumeSchemaTests extends PulsarListenerTestsBase {

	static final String STRING_TOPIC = "placst-str-topic";
	static CountDownLatch stringLatch = new CountDownLatch(3);
	static List<String> stringMessages = new ArrayList<>();

	static final String JSON_TOPIC = "placst-json-topic";
	static CountDownLatch jsonLatch = new CountDownLatch(3);
	static List<Map<String, Object>> jsonMessages = new ArrayList<>();

	static final String AVRO_TOPIC = "placst-avro-topic";
	static CountDownLatch avroLatch = new CountDownLatch(3);
	static List<Map<String, Object>> avroMessages = new ArrayList<>();

	static final String KEYVALUE_TOPIC = "placst-kv-topic";
	static CountDownLatch keyValueLatch = new CountDownLatch(3);
	static List<Map<String, Object>> keyValueMessages = new ArrayList<>();

	@Test
	void stringSchema() throws Exception {
		var pulsarProducerFactory = new DefaultPulsarProducerFactory<String>(pulsarClient);
		var template = new PulsarTemplate<>(pulsarProducerFactory);
		var expectedMessages = new ArrayList<String>();
		for (int i = 0; i < 3; i++) {
			var msg = "str-" + i;
			template.send(STRING_TOPIC, msg, Schema.STRING);
			expectedMessages.add(msg);
		}
		assertThat(stringLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(stringMessages).containsExactlyInAnyOrderElementsOf(expectedMessages);
	}

	@Test
	void jsonSchema() throws Exception {
		var pulsarProducerFactory = new DefaultPulsarProducerFactory<UserRecord>(pulsarClient);
		var template = new PulsarTemplate<>(pulsarProducerFactory);
		var schema = JSONSchema.of(UserRecord.class);
		var expectedMessages = new ArrayList<Map<String, Object>>();
		for (int i = 0; i < 3; i++) {
			var user = new UserRecord("Jason", i);
			template.send(JSON_TOPIC, user, schema);
			expectedMessages.add(Map.of("name", user.name(), "age", user.age()));
		}
		assertThat(jsonLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(jsonMessages).containsExactlyInAnyOrderElementsOf(expectedMessages);
	}

	@Test
	void avroSchema() throws Exception {
		var pulsarProducerFactory = new DefaultPulsarProducerFactory<UserPojo>(pulsarClient);
		var template = new PulsarTemplate<>(pulsarProducerFactory);
		var schema = AvroSchema.of(UserPojo.class);
		var expectedMessages = new ArrayList<Map<String, Object>>();
		for (int i = 0; i < 3; i++) {
			var user = new UserPojo("Avi", i);
			template.send(AVRO_TOPIC, user, schema);
			expectedMessages.add(Map.of("name", user.getName(), "age", user.getAge()));
		}
		assertThat(avroLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(avroMessages).containsExactlyInAnyOrderElementsOf(expectedMessages);
	}

	@Test
	void keyValueSchema() throws Exception {
		var pulsarProducerFactory = new DefaultPulsarProducerFactory<KeyValue<String, Integer>>(pulsarClient);
		var template = new PulsarTemplate<>(pulsarProducerFactory);
		var kvSchema = Schema.KeyValue(Schema.STRING, Schema.INT32, KeyValueEncodingType.INLINE);
		var expectedMessages = new ArrayList<Map<String, Object>>();
		for (int i = 0; i < 3; i++) {
			var kv = new KeyValue<>("Kevin", i);
			template.send(KEYVALUE_TOPIC, kv, kvSchema);
			expectedMessages.add(Map.of(kv.getKey(), kv.getValue()));
		}
		assertThat(keyValueLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(keyValueMessages).containsExactlyInAnyOrderElementsOf(expectedMessages);
	}

	@EnablePulsar
	@Configuration
	static class PulsarListenerAutoConsumeSchemaTestsConfig {

		@PulsarListener(id = "stringAcListener", topics = STRING_TOPIC, schemaType = SchemaType.AUTO_CONSUME,
				properties = { "subscriptionInitialPosition=Earliest" })
		void listenString(Message<GenericRecord> genericMessage) {
			assertThat(genericMessage.getValue().getNativeObject()).isInstanceOf(String.class);
			stringMessages.add(genericMessage.getValue().getNativeObject().toString());
			stringLatch.countDown();
		}

		@PulsarListener(id = "jsonAcListener", topics = JSON_TOPIC, schemaType = SchemaType.AUTO_CONSUME,
				properties = { "subscriptionInitialPosition=Earliest" })
		void listenJson(Message<GenericRecord> genericMessage) {
			assertThat(genericMessage.getValue()).isInstanceOf(GenericJsonRecord.class);
			GenericJsonRecord record = (GenericJsonRecord) genericMessage.getValue();
			assertThat(record.getSchemaType()).isEqualTo(SchemaType.JSON);
			assertThat(record).extracting("schemaInfo")
				.satisfies((obj) -> assertThat(obj.toString()).contains("\"name\": \"UserRecord\""));
			jsonMessages.add(record.getFields()
				.stream()
				.map(Field::getName)
				.collect(Collectors.toMap(Function.identity(), record::getField)));
			jsonLatch.countDown();
		}

		@PulsarListener(id = "avroAcListener", topics = AVRO_TOPIC, schemaType = SchemaType.AUTO_CONSUME,
				properties = { "subscriptionInitialPosition=Earliest" })
		void listenAvro(Message<GenericRecord> genericMessage) {
			assertThat(genericMessage.getValue()).isInstanceOf(GenericAvroRecord.class);
			GenericAvroRecord record = (GenericAvroRecord) genericMessage.getValue();
			assertThat(record.getSchemaType()).isEqualTo(SchemaType.AVRO);
			assertThat(record).extracting("schema")
				.satisfies((obj) -> assertThat(obj.toString()).contains("\"name\":\"UserPojo\""));
			avroMessages.add(record.getFields()
				.stream()
				.map(Field::getName)
				.collect(Collectors.toMap(Function.identity(), record::getField)));
			avroLatch.countDown();
		}

		@SuppressWarnings("unchecked")
		@PulsarListener(id = "keyvalueAcListener", topics = KEYVALUE_TOPIC, schemaType = SchemaType.AUTO_CONSUME,
				properties = { "subscriptionInitialPosition=Earliest" })
		void listenKeyvalue(Message<GenericRecord> genericMessage) {
			assertThat(genericMessage.getValue().getSchemaType()).isEqualTo(SchemaType.KEY_VALUE);
			assertThat(genericMessage.getValue().getNativeObject()).isInstanceOf(KeyValue.class);
			var record = (KeyValue<String, Object>) genericMessage.getValue().getNativeObject();
			keyValueMessages.add(Map.of(record.getKey(), record.getValue()));
			keyValueLatch.countDown();
		}

	}

}
