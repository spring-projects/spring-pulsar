/*
 * Copyright 2012-2023 the original author or authors.
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

package org.springframework.pulsar.inttest.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.test.support.PulsarTestContainerSupport;
import org.springframework.test.context.ActiveProfiles;

@Testcontainers(disabledWithoutDocker = true)
@ExtendWith(OutputCaptureExtension.class)
class DefaultTenantAndNamespaceTests {

	@SuppressWarnings("unused")
	@Container
	@ServiceConnection
	static PulsarContainer PULSAR_CONTAINER = new PulsarContainer(PulsarTestContainerSupport.getPulsarImage());

	@Nested
	@SpringBootTest(classes = ImperativeAppConfig.class,
			properties = { "spring.pulsar.defaults.topic.tenant=my-tenant-i",
					"spring.pulsar.defaults.topic.namespace=my-namespace-i" })
	@ExtendWith(OutputCaptureExtension.class)
	@ActiveProfiles("inttest.pulsar.imperative")
	class WithImperativeApp {

		@Test
		void produceConsumeWithDefaultTenantNamespace(CapturedOutput output,
				@Autowired PulsarAdministration pulsarAdmin) {
			TestVerifyUtils.verifyProduceConsume(output, 10, (i) -> ImperativeAppConfig.MSG_PREFIX + i);
			TestVerifyUtils.verifyTopicsLocatedInTenantAndNamespace(pulsarAdmin, ImperativeAppConfig.TENANT,
					ImperativeAppConfig.NAMESPACE, ImperativeAppConfig.NFQ_TOPIC);
		}

	}

	@Nested
	@SpringBootTest(classes = ReactiveAppConfig.class,
			properties = { "spring.pulsar.defaults.topic.tenant=my-tenant-r",
					"spring.pulsar.defaults.topic.namespace=my-namespace-r" })
	@ExtendWith(OutputCaptureExtension.class)
	@ActiveProfiles("inttest.pulsar.reactive")
	class WithReactiveApp {

		@Test
		void produceConsumeWithDefaultTenantNamespace(CapturedOutput output,
				@Autowired PulsarAdministration pulsarAdmin) {
			TestVerifyUtils.verifyProduceConsume(output, 10, (i) -> ReactiveAppConfig.MSG_PREFIX + i);
			TestVerifyUtils.verifyTopicsLocatedInTenantAndNamespace(pulsarAdmin, ReactiveAppConfig.TENANT,
					ReactiveAppConfig.NAMESPACE, ReactiveAppConfig.NFQ_TOPIC);
		}

	}

	private static class TestVerifyUtils {

		static void verifyProduceConsume(CapturedOutput output, int numExpectedMessages,
				Function<Integer, Object> expectedMessageFactory) {
			var expectedOutput = new ArrayList<String>();
			IntStream.range(0, numExpectedMessages).forEachOrdered((i) -> {
				var expectedMsg = expectedMessageFactory.apply(i);
				expectedOutput.add("++++++PRODUCE %s------".formatted(expectedMsg));
				expectedOutput.add("++++++CONSUME %s------".formatted(expectedMsg));
			});
			Awaitility.waitAtMost(Duration.ofSeconds(15))
				.untilAsserted(() -> assertThat(output).contains(expectedOutput));
		}

		static void verifyTopicsLocatedInTenantAndNamespace(PulsarAdministration pulsarAdmin, String tenant,
				String namespace, String topic) {
			// verify topics created in expected tenant/namespace and not in
			// public/default
			try (var admin = pulsarAdmin.createAdminClient()) {
				var fqTopic = "persistent://%s/%s/%s".formatted(tenant, namespace, topic);
				assertThat(admin.topics().getList("%s/%s".formatted(tenant, namespace))).containsExactly(fqTopic);
				assertThat(admin.topics().getList("public/default"))
					.noneSatisfy(t -> assertThat(t).doesNotEndWith("/" + topic));
			}
			catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}

	}

}
