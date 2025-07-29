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

package org.springframework.pulsar.function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.Sinks;
import org.apache.pulsar.client.admin.Sources;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.springframework.pulsar.PulsarException;
import org.springframework.pulsar.function.PulsarFunctionOperations.FunctionStopPolicy;
import org.springframework.pulsar.function.PulsarFunctionOperations.FunctionType;

/**
 * Tests for all &quot;Pulsar Functions&quot; (ie. {@link PulsarFunction},
 * {@link PulsarSink}, and {@link PulsarSource}).
 *
 * @author Chris Bono
 */
class PulsarFunctionsTests {

	private static final String TENANT = "tenant1";

	private static final String NAMESPACE = "namespace1";

	private PulsarAdmin pulsarAdmin = mock(PulsarAdmin.class, Mockito.RETURNS_DEEP_STUBS);

	@Nested
	class PulsarFunctionApi {

		private static final String NAME = "function1";

		private static final String JAR = "function1.jar";

		private FunctionConfig functionConfig = FunctionConfig.builder()
			.tenant(TENANT)
			.namespace(NAMESPACE)
			.name(NAME)
			.jar(JAR)
			.build();

		private PulsarFunction function = new PulsarFunction(functionConfig, null);

		@Test
		void accessors() {
			PulsarFunction function = new PulsarFunction(functionConfig, FunctionStopPolicy.STOP, null);
			assertThat(function.config()).isEqualTo(functionConfig);
			assertThat(function.name()).isEqualTo(NAME);
			assertThat(function.type()).isEqualTo(FunctionType.FUNCTION);
			assertThat(function.archive()).isEqualTo(JAR);
			assertThat(function.stopPolicy()).isEqualTo(FunctionStopPolicy.STOP);
		}

		@Test
		void defaultStopPolicy() {
			assertThat(function.stopPolicy()).isEqualTo(FunctionStopPolicy.DELETE);
		}

		@Test
		void get() throws PulsarAdminException {
			when(pulsarAdmin.functions().getFunction(anyString(), anyString(), anyString())).thenReturn(functionConfig);
			assertThat(function.get(pulsarAdmin)).isSameAs(functionConfig);
			verify(pulsarAdmin.functions()).getFunction(TENANT, NAMESPACE, NAME);
		}

		@Test
		void getIfExistsWithExistingFunction() throws PulsarAdminException {
			when(pulsarAdmin.functions().getFunction(anyString(), anyString(), anyString())).thenReturn(functionConfig);
			assertThat(function.getIfExists(pulsarAdmin)).hasValue(functionConfig);
			assertThat(function.functionExists(pulsarAdmin)).isTrue();
		}

		@Test
		void getIfExistsWithNonExistentFunction() throws PulsarAdminException {
			when(pulsarAdmin.functions().getFunction(anyString(), anyString(), anyString()))
					.thenThrow(new NotFoundException(null, "400", 400));
			assertThat(function.getIfExists(pulsarAdmin)).isEmpty();
			assertThat(function.functionExists(pulsarAdmin)).isFalse();
		}

		@Test
		void create() throws PulsarAdminException {
			function.create(pulsarAdmin);
			verify(pulsarAdmin.functions()).createFunction(functionConfig, JAR);
		}

		@Test
		void createWithUrl() throws PulsarAdminException {
			function.createWithUrl(pulsarAdmin);
			verify(pulsarAdmin.functions()).createFunctionWithUrl(functionConfig, JAR);
		}

		@Test
		void update() throws PulsarAdminException {
			function.update(pulsarAdmin);
			verify(pulsarAdmin.functions()).updateFunction(functionConfig, JAR, null);
		}

		@Test
		void updateWithUrl() throws PulsarAdminException {
			function.updateWithUrl(pulsarAdmin);
			verify(pulsarAdmin.functions()).updateFunctionWithUrl(functionConfig, JAR, null);
		}

		@Test
		void stop() throws PulsarAdminException {
			function.stop(pulsarAdmin);
			verify(pulsarAdmin.functions()).stopFunction(TENANT, NAMESPACE, NAME);
		}

		@Test
		void stopWrapsPulsarAdminException() throws PulsarAdminException {
			Functions functions = mock(Functions.class);
			when(pulsarAdmin.functions()).thenReturn(functions);
			PulsarAdminException paex = new PulsarAdminException("bad-stop");
			doThrow(paex).when(functions).stopFunction(anyString(), anyString(), anyString());
			assertThatThrownBy(() -> function.stop(pulsarAdmin)).isInstanceOf(PulsarException.class)
				.hasMessageContaining("bad-stop")
				.hasCause(paex);
		}

		@Test
		void delete() throws PulsarAdminException {
			function.delete(pulsarAdmin);
			verify(pulsarAdmin.functions()).deleteFunction(TENANT, NAMESPACE, NAME);
		}

		@Test
		void deleteWrapsPulsarAdminException() throws PulsarAdminException {
			PulsarFunction function = new PulsarFunction(functionConfig, null);
			Functions functions = mock(Functions.class);
			when(pulsarAdmin.functions()).thenReturn(functions);
			PulsarAdminException paex = new PulsarAdminException("bad-delete");
			doThrow(paex).when(functions).deleteFunction(anyString(), anyString(), anyString());
			assertThatThrownBy(() -> function.delete(pulsarAdmin)).isInstanceOf(PulsarException.class)
				.hasMessageContaining("bad-delete")
				.hasCause(paex);
		}

	}

	@Nested
	class PulsarSinkApi {

		private static final String NAME = "sink1";

		private static final String JAR = "sink1.jar";

		private SinkConfig sinkConfig = SinkConfig.builder()
			.tenant(TENANT)
			.namespace(NAMESPACE)
			.name(NAME)
			.archive(JAR)
			.build();

		private PulsarSink sink = new PulsarSink(sinkConfig, null);

		@Test
		void accessors() {
			PulsarSink sink = new PulsarSink(sinkConfig, FunctionStopPolicy.STOP, null);
			assertThat(sink.config()).isEqualTo(sinkConfig);
			assertThat(sink.name()).isEqualTo(NAME);
			assertThat(sink.type()).isEqualTo(FunctionType.SINK);
			assertThat(sink.archive()).isEqualTo(JAR);
			assertThat(sink.stopPolicy()).isEqualTo(FunctionStopPolicy.STOP);
		}

		@Test
		void defaultStopPolicy() {
			assertThat(sink.stopPolicy()).isEqualTo(FunctionStopPolicy.DELETE);
		}

		@Test
		void get() throws PulsarAdminException {
			when(pulsarAdmin.sinks().getSink(anyString(), anyString(), anyString())).thenReturn(sinkConfig);
			assertThat(sink.get(pulsarAdmin)).isSameAs(sinkConfig);
			verify(pulsarAdmin.sinks()).getSink(TENANT, NAMESPACE, NAME);
		}

		@Test
		void getIfExistsWithExistingSink() throws PulsarAdminException {
			when(pulsarAdmin.sinks().getSink(anyString(), anyString(), anyString())).thenReturn(sinkConfig);
			assertThat(sink.getIfExists(pulsarAdmin)).hasValue(sinkConfig);
			assertThat(sink.functionExists(pulsarAdmin)).isTrue();
		}

		@Test
		void getIfExistsWithNonExistentSink() throws PulsarAdminException {
			when(pulsarAdmin.sinks().getSink(anyString(), anyString(), anyString()))
					.thenThrow(new NotFoundException(null, "400", 400));
			assertThat(sink.getIfExists(pulsarAdmin)).isEmpty();
			assertThat(sink.functionExists(pulsarAdmin)).isFalse();
		}

		@Test
		void create() throws PulsarAdminException {
			sink.create(pulsarAdmin);
			verify(pulsarAdmin.sinks()).createSink(sinkConfig, JAR);
		}

		@Test
		void createWithUrl() throws PulsarAdminException {
			sink.createWithUrl(pulsarAdmin);
			verify(pulsarAdmin.sinks()).createSinkWithUrl(sinkConfig, JAR);
		}

		@Test
		void update() throws PulsarAdminException {
			sink.update(pulsarAdmin);
			verify(pulsarAdmin.sinks()).updateSink(sinkConfig, JAR, null);
		}

		@Test
		void updateWithUrl() throws PulsarAdminException {
			sink.updateWithUrl(pulsarAdmin);
			verify(pulsarAdmin.sinks()).updateSinkWithUrl(sinkConfig, JAR, null);
		}

		@Test
		void stop() throws PulsarAdminException {
			sink.stop(pulsarAdmin);
			verify(pulsarAdmin.sinks()).stopSink(TENANT, NAMESPACE, NAME);
		}

		@Test
		void stopWrapsPulsarAdminException() throws PulsarAdminException {
			Sinks sinks = mock(Sinks.class);
			when(pulsarAdmin.sinks()).thenReturn(sinks);
			PulsarAdminException paex = new PulsarAdminException("bad-stop");
			doThrow(paex).when(sinks).stopSink(anyString(), anyString(), anyString());
			assertThatThrownBy(() -> sink.stop(pulsarAdmin)).isInstanceOf(PulsarException.class)
				.hasMessageContaining("bad-stop")
				.hasCause(paex);
		}

		@Test
		void delete() throws PulsarAdminException {
			sink.delete(pulsarAdmin);
			verify(pulsarAdmin.sinks()).deleteSink(TENANT, NAMESPACE, NAME);
		}

		@Test
		void deleteWrapsPulsarAdminException() throws PulsarAdminException {
			PulsarSink sink = new PulsarSink(sinkConfig, null);
			Sinks sinks = mock(Sinks.class);
			when(pulsarAdmin.sinks()).thenReturn(sinks);
			PulsarAdminException paex = new PulsarAdminException("bad-delete");
			doThrow(paex).when(sinks).deleteSink(anyString(), anyString(), anyString());
			assertThatThrownBy(() -> sink.delete(pulsarAdmin)).isInstanceOf(PulsarException.class)
				.hasMessageContaining("bad-delete")
				.hasCause(paex);
		}

	}

	@Nested
	class PulsarSourceApi {

		private static final String NAME = "source1";

		private static final String JAR = "source1.jar";

		private SourceConfig sourceConfig = SourceConfig.builder()
			.tenant(TENANT)
			.namespace(NAMESPACE)
			.name(NAME)
			.archive(JAR)
			.build();

		private PulsarSource source = new PulsarSource(sourceConfig, null);

		@Test
		void accessors() {
			PulsarSource source = new PulsarSource(sourceConfig, FunctionStopPolicy.STOP, null);
			assertThat(source.config()).isEqualTo(sourceConfig);
			assertThat(source.name()).isEqualTo(NAME);
			assertThat(source.type()).isEqualTo(FunctionType.SOURCE);
			assertThat(source.archive()).isEqualTo(JAR);
			assertThat(source.stopPolicy()).isEqualTo(FunctionStopPolicy.STOP);
		}

		@Test
		void defaultStopPolicy() {
			assertThat(source.stopPolicy()).isEqualTo(FunctionStopPolicy.DELETE);
		}

		@Test
		void get() throws PulsarAdminException {
			when(pulsarAdmin.sources().getSource(anyString(), anyString(), anyString())).thenReturn(sourceConfig);
			assertThat(source.get(pulsarAdmin)).isSameAs(sourceConfig);
			verify(pulsarAdmin.sources()).getSource(TENANT, NAMESPACE, NAME);
		}

		@Test
		void getIfExistsWithExistingSource() throws PulsarAdminException {
			when(pulsarAdmin.sources().getSource(anyString(), anyString(), anyString())).thenReturn(sourceConfig);
			assertThat(source.getIfExists(pulsarAdmin)).hasValue(sourceConfig);
			assertThat(source.functionExists(pulsarAdmin)).isTrue();
		}

		@Test
		void getIfExistsWithNonExistentSource() throws PulsarAdminException {
			when(pulsarAdmin.sources().getSource(anyString(), anyString(), anyString()))
					.thenThrow(new NotFoundException(null, "400", 400));
			assertThat(source.getIfExists(pulsarAdmin)).isEmpty();
			assertThat(source.functionExists(pulsarAdmin)).isFalse();
		}

		@Test
		void create() throws PulsarAdminException {
			source.create(pulsarAdmin);
			verify(pulsarAdmin.sources()).createSource(sourceConfig, JAR);
		}

		@Test
		void createWithUrl() throws PulsarAdminException {
			source.createWithUrl(pulsarAdmin);
			verify(pulsarAdmin.sources()).createSourceWithUrl(sourceConfig, JAR);
		}

		@Test
		void update() throws PulsarAdminException {
			source.update(pulsarAdmin);
			verify(pulsarAdmin.sources()).updateSource(sourceConfig, JAR, null);
		}

		@Test
		void updateWithUrl() throws PulsarAdminException {
			source.updateWithUrl(pulsarAdmin);
			verify(pulsarAdmin.sources()).updateSourceWithUrl(sourceConfig, JAR, null);
		}

		@Test
		void stop() throws PulsarAdminException {
			source.stop(pulsarAdmin);
			verify(pulsarAdmin.sources()).stopSource(TENANT, NAMESPACE, NAME);
		}

		@Test
		void stopWrapsPulsarAdminException() throws PulsarAdminException {
			Sources sources = mock(Sources.class);
			when(pulsarAdmin.sources()).thenReturn(sources);
			PulsarAdminException paex = new PulsarAdminException("bad-stop");
			doThrow(paex).when(sources).stopSource(anyString(), anyString(), anyString());
			assertThatThrownBy(() -> source.stop(pulsarAdmin)).isInstanceOf(PulsarException.class)
				.hasMessageContaining("bad-stop")
				.hasCause(paex);
		}

		@Test
		void delete() throws PulsarAdminException {
			source.delete(pulsarAdmin);
			verify(pulsarAdmin.sources()).deleteSource(TENANT, NAMESPACE, NAME);
		}

		@Test
		void deleteWrapsPulsarAdminException() throws PulsarAdminException {
			PulsarSource source = new PulsarSource(sourceConfig, null);
			Sources sources = mock(Sources.class);
			when(pulsarAdmin.sources()).thenReturn(sources);
			PulsarAdminException paex = new PulsarAdminException("bad-delete");
			doThrow(paex).when(sources).deleteSource(anyString(), anyString(), anyString());
			assertThatThrownBy(() -> source.delete(pulsarAdmin)).isInstanceOf(PulsarException.class)
				.hasMessageContaining("bad-delete")
				.hasCause(paex);
		}

	}

}
