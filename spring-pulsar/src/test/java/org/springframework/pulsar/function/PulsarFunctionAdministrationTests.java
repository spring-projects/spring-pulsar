/*
 * Copyright 2023-2023 the original author or authors.
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
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.pulsar.PulsarException;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.function.PulsarFunctionAdministration.PulsarFunctionException;

/**
 * Tests for {@link PulsarFunctionAdministration}.
 *
 * @author Chris Bono
 */
class PulsarFunctionAdministrationTests {

	private PulsarAdmin pulsarAdmin = mock(PulsarAdmin.class, Mockito.RETURNS_DEEP_STUBS);

	private PulsarAdministration springPulsarAdmin = mock(PulsarAdministration.class);

	private PulsarFunctionAdministration functionAdmin;

	private StaticListableBeanFactory beanFactory;

	@BeforeEach
	void setupAdminsAndBeanFactory() throws PulsarClientException {
		when(springPulsarAdmin.createAdminClient()).thenReturn(pulsarAdmin);
		beanFactory = new StaticListableBeanFactory();
		functionAdmin = new PulsarFunctionAdministration(springPulsarAdmin,
				beanFactory.getBeanProvider(PulsarFunction.class), beanFactory.getBeanProvider(PulsarSink.class),
				beanFactory.getBeanProvider(PulsarSource.class), true, true);
	}

	@Test
	void noFunctionsSinksOrSourcesProvided() throws PulsarClientException {
		functionAdmin.createOrUpdateUserDefinedFunctions();
		verify(springPulsarAdmin).createAdminClient();
		verifyNoMoreInteractions(springPulsarAdmin);
		verify(pulsarAdmin).close();
		verifyNoMoreInteractions(pulsarAdmin);
	}

	@Nested
	class ManagePulsarFunctions {

		private FunctionConfig functionConfig = FunctionConfig.builder().tenant("tenant1").namespace("namespace1")
				.name("function1").jar("function1.jar").build();

		@Test
		void createFunction() throws PulsarAdminException {
			when(pulsarAdmin.functions().getFunction("tenant1", "namespace1", "function1"))
					.thenThrow(new NotFoundException(null, "400", 400));
			beanFactory.addBean("myFunction", new PulsarFunction(functionConfig, null));

			functionAdmin.createOrUpdateUserDefinedFunctions();

			verify(pulsarAdmin.functions()).getFunction(functionConfig.getTenant(), functionConfig.getNamespace(),
					functionConfig.getName());
			verify(pulsarAdmin.functions()).createFunction(functionConfig, functionConfig.getJar());
		}

		@Test
		void updateFunction() throws PulsarAdminException {
			when(pulsarAdmin.functions().getFunction("tenant1", "namespace1", "function1")).thenReturn(functionConfig);
			FunctionConfig functionConfigNew = functionConfig.toBuilder().jar("function1-v2.jar").build();
			beanFactory.addBean("myFunction", new PulsarFunction(functionConfigNew, null));

			functionAdmin.createOrUpdateUserDefinedFunctions();

			verify(pulsarAdmin.functions()).getFunction(functionConfigNew.getTenant(), functionConfigNew.getNamespace(),
					functionConfigNew.getName());
			verify(pulsarAdmin.functions()).updateFunction(functionConfigNew, functionConfigNew.getJar(), null);
		}

	}

	@Nested
	class ManagePulsarSinks {

		private SinkConfig sinkConfig = SinkConfig.builder().tenant("tenant1").namespace("namespace1").name("sink1")
				.archive("sink1.jar").build();

		@Test
		void createSink() throws PulsarAdminException {
			when(pulsarAdmin.sinks().getSink("tenant1", "namespace1", "sink1"))
					.thenThrow(new NotFoundException(null, "400", 400));

			beanFactory.addBean("mySink", new PulsarSink(sinkConfig, null));

			functionAdmin.createOrUpdateUserDefinedFunctions();

			verify(pulsarAdmin.sinks()).getSink(sinkConfig.getTenant(), sinkConfig.getNamespace(),
					sinkConfig.getName());
			verify(pulsarAdmin.sinks()).createSink(sinkConfig, sinkConfig.getArchive());
		}

		@Test
		void updateSink() throws PulsarAdminException {
			when(pulsarAdmin.sinks().getSink("tenant1", "namespace1", "sink1")).thenReturn(sinkConfig);
			SinkConfig sinkConfigNew = sinkConfig.toBuilder().archive("sink1-v2.jar").build();
			UpdateOptions updateOptions = new UpdateOptionsImpl();
			beanFactory.addBean("mySink", new PulsarSink(sinkConfigNew, updateOptions));

			functionAdmin.createOrUpdateUserDefinedFunctions();

			verify(pulsarAdmin.sinks()).getSink(sinkConfigNew.getTenant(), sinkConfigNew.getNamespace(),
					sinkConfigNew.getName());
			verify(pulsarAdmin.sinks()).updateSink(sinkConfigNew, sinkConfigNew.getArchive(), updateOptions);
		}

	}

	@Nested
	class ManagePulsarSources {

		private SourceConfig sourceConfig = SourceConfig.builder().tenant("tenant1").namespace("namespace1")
				.name("source1").archive("source1.jar").build();

		@Test
		void createSource() throws PulsarAdminException {
			when(pulsarAdmin.sources().getSource("tenant1", "namespace1", "source1"))
					.thenThrow(new NotFoundException(null, "400", 400));
			beanFactory.addBean("mySource", new PulsarSource(sourceConfig, null));

			functionAdmin.createOrUpdateUserDefinedFunctions();

			verify(pulsarAdmin.sources()).getSource(sourceConfig.getTenant(), sourceConfig.getNamespace(),
					sourceConfig.getName());
			verify(pulsarAdmin.sources()).createSource(sourceConfig, sourceConfig.getArchive());
		}

		@Test
		void updateSource() throws PulsarAdminException {
			when(pulsarAdmin.sources().getSource("tenant1", "namespace1", "source1")).thenReturn(sourceConfig);
			SourceConfig sourceConfigNew = sourceConfig.toBuilder().archive("source1-v2.jar").build();
			UpdateOptions updateOptions = new UpdateOptionsImpl();
			beanFactory.addBean("mySource", new PulsarSource(sourceConfigNew, updateOptions));

			functionAdmin.createOrUpdateUserDefinedFunctions();

			verify(pulsarAdmin.sources()).getSource(sourceConfigNew.getTenant(), sourceConfigNew.getNamespace(),
					sourceConfigNew.getName());
			verify(pulsarAdmin.sources()).updateSource(sourceConfigNew, sourceConfigNew.getArchive(), updateOptions);
		}

	}

	@Nested
	class ProcessHandling {

		private PulsarFunction function1;

		private PulsarSink sink1;

		private PulsarSource source1;

		@BeforeEach
		void setupFunctionsSinksAndSources() throws PulsarAdminException {

			function1 = mock(PulsarFunction.class);
			when(function1.functionExists(pulsarAdmin)).thenReturn(false);

			sink1 = mock(PulsarSink.class);
			when(sink1.functionExists(pulsarAdmin)).thenReturn(false);

			source1 = mock(PulsarSource.class);
			when(source1.functionExists(pulsarAdmin)).thenReturn(false);

			beanFactory.addBean("function1", function1);
			beanFactory.addBean("sink1", sink1);
			beanFactory.addBean("source1", source1);
		}

		@Test
		void allFunctionsProcessedSuccessfully() throws PulsarAdminException {
			functionAdmin.createOrUpdateUserDefinedFunctions();
			verify(function1).create(pulsarAdmin);
			verify(sink1).create(pulsarAdmin);
			verify(source1).create(pulsarAdmin);
		}

		@Test
		void firstProcessedFunctionFailsFast() throws PulsarAdminException {
			PulsarAdminException ex = new PulsarAdminException("BOOM");
			when(function1.functionExists(pulsarAdmin)).thenThrow(ex);
			PulsarFunctionException thrown = catchThrowableOfType(
					() -> functionAdmin.createOrUpdateUserDefinedFunctions(), PulsarFunctionException.class);
			assertThat(thrown.getFailures()).containsExactly(entry(function1, ex));
			verify(function1, never()).create(pulsarAdmin);
			verify(function1, never()).update(pulsarAdmin);
			verifyNoInteractions(sink1, source1);
		}

		@Test
		void middleProcessedFunctionFailsFast() throws PulsarAdminException {
			PulsarAdminException ex = new PulsarAdminException("BOOM");
			when(sink1.functionExists(pulsarAdmin)).thenThrow(ex);
			PulsarFunctionException thrown = catchThrowableOfType(
					() -> functionAdmin.createOrUpdateUserDefinedFunctions(), PulsarFunctionException.class);
			assertThat(thrown.getFailures()).containsExactly(entry(sink1, ex));
			verify(function1).create(pulsarAdmin);
			verify(sink1, never()).create(pulsarAdmin);
			verify(sink1, never()).update(pulsarAdmin);
			verifyNoInteractions(source1);
		}

		@Test
		void lastProcessedFunctionFailsFast() throws PulsarAdminException {
			PulsarAdminException ex = new PulsarAdminException("BOOM");
			when(source1.functionExists(pulsarAdmin)).thenThrow(ex);
			PulsarFunctionException thrown = catchThrowableOfType(
					() -> functionAdmin.createOrUpdateUserDefinedFunctions(), PulsarFunctionException.class);
			assertThat(thrown.getFailures()).containsExactly(entry(source1, ex));
			verify(function1).create(pulsarAdmin);
			verify(sink1).create(pulsarAdmin);
			verify(source1, never()).create(pulsarAdmin);
			verify(source1, never()).update(pulsarAdmin);
		}

		@Test
		void firstProcessedFunctionFailsSlow() throws PulsarAdminException {
			functionAdmin = new PulsarFunctionAdministration(springPulsarAdmin,
					beanFactory.getBeanProvider(PulsarFunction.class), beanFactory.getBeanProvider(PulsarSink.class),
					beanFactory.getBeanProvider(PulsarSource.class), false, true);
			PulsarAdminException ex = new PulsarAdminException("BOOM");
			when(function1.functionExists(pulsarAdmin)).thenThrow(ex);
			PulsarFunctionException thrown = catchThrowableOfType(
					() -> functionAdmin.createOrUpdateUserDefinedFunctions(), PulsarFunctionException.class);
			assertThat(thrown.getFailures()).containsExactly(entry(function1, ex));
			verify(function1, never()).create(pulsarAdmin);
			verify(function1, never()).update(pulsarAdmin);
			verify(sink1).create(pulsarAdmin);
			verify(source1).create(pulsarAdmin);
		}

		@Test
		void middleProcessedFunctionFailsSlow() throws PulsarAdminException {
			functionAdmin = new PulsarFunctionAdministration(springPulsarAdmin,
					beanFactory.getBeanProvider(PulsarFunction.class), beanFactory.getBeanProvider(PulsarSink.class),
					beanFactory.getBeanProvider(PulsarSource.class), false, true);
			PulsarAdminException ex = new PulsarAdminException("BOOM");
			when(sink1.functionExists(pulsarAdmin)).thenThrow(ex);
			PulsarFunctionException thrown = catchThrowableOfType(
					() -> functionAdmin.createOrUpdateUserDefinedFunctions(), PulsarFunctionException.class);
			assertThat(thrown.getFailures()).containsExactly(entry(sink1, ex));
			verify(function1).create(pulsarAdmin);
			verify(sink1, never()).create(pulsarAdmin);
			verify(sink1, never()).update(pulsarAdmin);
			verify(source1).create(pulsarAdmin);
		}

		@Test
		void lastProcessedFunctionFailsSlow() throws PulsarAdminException {
			functionAdmin = new PulsarFunctionAdministration(springPulsarAdmin,
					beanFactory.getBeanProvider(PulsarFunction.class), beanFactory.getBeanProvider(PulsarSink.class),
					beanFactory.getBeanProvider(PulsarSource.class), false, true);
			PulsarAdminException ex = new PulsarAdminException("BOOM");
			when(source1.functionExists(pulsarAdmin)).thenThrow(ex);
			PulsarFunctionException thrown = catchThrowableOfType(
					() -> functionAdmin.createOrUpdateUserDefinedFunctions(), PulsarFunctionException.class);
			assertThat(thrown.getFailures()).containsExactly(entry(source1, ex));
			verify(function1).create(pulsarAdmin);
			verify(sink1).create(pulsarAdmin);
			verify(source1, never()).create(pulsarAdmin);
			verify(source1, never()).update(pulsarAdmin);
		}

		@Test
		void allProcessedFunctionsFailSlow() throws PulsarAdminException {
			functionAdmin = new PulsarFunctionAdministration(springPulsarAdmin,
					beanFactory.getBeanProvider(PulsarFunction.class), beanFactory.getBeanProvider(PulsarSink.class),
					beanFactory.getBeanProvider(PulsarSource.class), false, true);
			PulsarAdminException ex1 = new PulsarAdminException("BOOM1");
			PulsarAdminException ex2 = new PulsarAdminException("BOOM2");
			PulsarAdminException ex3 = new PulsarAdminException("BOOM3");
			when(function1.functionExists(pulsarAdmin)).thenThrow(ex1);
			when(sink1.functionExists(pulsarAdmin)).thenThrow(ex2);
			when(source1.functionExists(pulsarAdmin)).thenThrow(ex3);
			PulsarFunctionException thrown = catchThrowableOfType(
					() -> functionAdmin.createOrUpdateUserDefinedFunctions(), PulsarFunctionException.class);
			assertThat(thrown.getFailures()).containsExactly(entry(function1, ex1), entry(sink1, ex2),
					entry(source1, ex3));
			verify(function1, never()).create(pulsarAdmin);
			verify(function1, never()).update(pulsarAdmin);
			verify(sink1, never()).create(pulsarAdmin);
			verify(sink1, never()).update(pulsarAdmin);
			verify(source1, never()).create(pulsarAdmin);
			verify(source1, never()).update(pulsarAdmin);
		}

		@Test
		void createAdminClientFails() throws PulsarClientException {
			when(springPulsarAdmin.createAdminClient()).thenThrow(new PulsarClientException("NOPE"));
			assertThatThrownBy(() -> functionAdmin.createOrUpdateUserDefinedFunctions())
					.isInstanceOf(PulsarException.class)
					.hasMessageContaining("Unable to create/update functions - could not create PulsarAdmin: NOPE");
		}

	}

	@Nested
	@ExtendWith(OutputCaptureExtension.class)
	class ProcessHandlingPropagationDisabled {

		@BeforeEach
		void setupFunctionAdminWithPropagationDisabled() {
			functionAdmin = new PulsarFunctionAdministration(springPulsarAdmin,
					beanFactory.getBeanProvider(PulsarFunction.class), beanFactory.getBeanProvider(PulsarSink.class),
					beanFactory.getBeanProvider(PulsarSource.class), true, false);
		}

		@Test
		void createAdminClientFails(CapturedOutput output) throws PulsarClientException {
			when(springPulsarAdmin.createAdminClient()).thenThrow(new PulsarClientException("NOPE"));
			functionAdmin.createOrUpdateUserDefinedFunctions();
			assertThat(output).contains("Unable to create/update functions - could not create PulsarAdmin: NOPE");
		}

		@Test
		void processedFunctionFails(CapturedOutput output) throws PulsarAdminException {
			PulsarFunction function1 = mock(PulsarFunction.class);
			beanFactory.addBean("function1", function1);
			PulsarAdminException ex = new PulsarAdminException("BOOM");
			when(function1.functionExists(pulsarAdmin)).thenThrow(ex);

			functionAdmin.createOrUpdateUserDefinedFunctions();

			assertThat(output).contains("Encountered 1 error(s) creating/updating functions:",
					"PulsarAdminException: BOOM");
		}

	}

}
