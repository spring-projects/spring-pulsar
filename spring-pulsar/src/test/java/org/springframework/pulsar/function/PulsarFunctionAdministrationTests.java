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

import static com.github.stefanbirkner.systemlambda.SystemLambda.tapSystemErrAndOutNormalized;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.mockito.Mockito;

import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.pulsar.PulsarException;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.function.PulsarFunctionAdministration.PulsarFunctionException;
import org.springframework.pulsar.function.PulsarFunctionOperations.FunctionStopPolicy;
import org.springframework.pulsar.function.PulsarFunctionOperations.FunctionType;

/**
 * Tests for {@link PulsarFunctionAdministration}.
 *
 * @author Chris Bono
 */
class PulsarFunctionAdministrationTests {

	private PulsarAdmin pulsarAdmin;

	private PulsarAdministration springPulsarAdmin;

	private PulsarFunctionAdministration functionAdmin;

	private StaticListableBeanFactory beanFactory;

	private PulsarFunction function1;

	private PulsarSink sink1;

	private PulsarSource source1;

	@BeforeEach
	void setupSharedMocks() throws PulsarClientException, PulsarAdminException {
		pulsarAdmin = mock(PulsarAdmin.class, Mockito.RETURNS_DEEP_STUBS);
		springPulsarAdmin = mock(PulsarAdministration.class);
		when(springPulsarAdmin.createAdminClient()).thenReturn(pulsarAdmin);
		beanFactory = new StaticListableBeanFactory();
		functionAdmin = new PulsarFunctionAdministration(springPulsarAdmin,
				beanFactory.getBeanProvider(PulsarFunction.class), beanFactory.getBeanProvider(PulsarSink.class),
				beanFactory.getBeanProvider(PulsarSource.class), true, true, true);
		// create function, sink, source mock but do not add to bean factory
		function1 = mock(PulsarFunction.class);
		when(function1.functionExists(pulsarAdmin)).thenReturn(false);
		when(function1.name()).thenReturn("function1");
		when(function1.type()).thenReturn(FunctionType.FUNCTION);
		sink1 = mock(PulsarSink.class);
		when(sink1.functionExists(pulsarAdmin)).thenReturn(false);
		when(sink1.name()).thenReturn("sink1");
		when(sink1.type()).thenReturn(FunctionType.SINK);
		source1 = mock(PulsarSource.class);
		when(source1.functionExists(pulsarAdmin)).thenReturn(false);
		when(source1.name()).thenReturn("source1");
		when(source1.type()).thenReturn(FunctionType.SOURCE);
	}

	@Nested
	class ProperCreateUpdateApiCalled {

		@ParameterizedTest
		@ValueSource(strings = { "myfunc.jar", "builtin://myfunc" })
		void create(String archive) throws PulsarAdminException {
			var function = setupMockFunction(archive);
			when(function.functionExists(pulsarAdmin)).thenReturn(false);
			functionAdmin.createOrUpdateUserDefinedFunctions();
			verify(function).create(pulsarAdmin);
		}

		@ParameterizedTest
		@ValueSource(strings = { "https://myfunc.jar", "file:///myfunc.jar", "function://myfunc.jar",
				"sink://myfunc.jar", "source://myfunc.jar" })
		void createWithUrl(String archive) throws PulsarAdminException {
			var function = setupMockFunction(archive);
			when(function.functionExists(pulsarAdmin)).thenReturn(false);
			functionAdmin.createOrUpdateUserDefinedFunctions();
			verify(function).createWithUrl(pulsarAdmin);
		}

		@ParameterizedTest
		@ValueSource(strings = { "myfunc.jar", "builtin://myfunc" })
		void update(String archive) throws PulsarAdminException {
			var function = setupMockFunction(archive);
			when(function.functionExists(pulsarAdmin)).thenReturn(true);
			functionAdmin.createOrUpdateUserDefinedFunctions();
			verify(function).update(pulsarAdmin);
		}

		@ParameterizedTest
		@ValueSource(strings = { "https://myfunc.jar", "file:///myfunc.jar", "function://myfunc.jar",
				"sink://myfunc.jar", "source://myfunc.jar" })
		void updateWithUrl(String archive) throws PulsarAdminException {
			var function = setupMockFunction(archive);
			when(function.functionExists(pulsarAdmin)).thenReturn(true);
			functionAdmin.createOrUpdateUserDefinedFunctions();
			verify(function).updateWithUrl(pulsarAdmin);
		}

		private PulsarFunction setupMockFunction(String archive) {
			var function = mock(PulsarFunction.class);
			when(function.name()).thenReturn("function1");
			when(function.type()).thenReturn(FunctionType.FUNCTION);
			when(function.archive()).thenReturn(archive);
			beanFactory.addBean("myFunction", function);
			return function;
		}

	}

	@Nested
	class ProperCreateUpdateProcessOrder {

		@Test
		void noFunctionsProvided() throws PulsarClientException {
			functionAdmin.createOrUpdateUserDefinedFunctions();
			verify(springPulsarAdmin, never()).createAdminClient();
			verifyNoInteractions(pulsarAdmin);
			assertThat(functionAdmin.getProcessedFunctions()).isEmpty();
		}

		@Test
		void functionProvided() {
			beanFactory.addBean("function1", function1);
			functionAdmin.createOrUpdateUserDefinedFunctions();
			assertThat(functionAdmin.getProcessedFunctions()).containsExactly(function1);
		}

		@Test
		void functionAndSinkProvided() {
			beanFactory.addBean("function1", function1);
			beanFactory.addBean("sink1", sink1);
			functionAdmin.createOrUpdateUserDefinedFunctions();
			assertThat(functionAdmin.getProcessedFunctions()).containsExactly(function1, sink1);
		}

		@Test
		void functionSinkAndSourceProvided() {
			beanFactory.addBean("function1", function1);
			beanFactory.addBean("sink1", sink1);
			beanFactory.addBean("source1", source1);
			functionAdmin.createOrUpdateUserDefinedFunctions();
			assertThat(functionAdmin.getProcessedFunctions()).containsExactly(function1, sink1, source1);
		}

		@Test
		void sinkProvided() {
			beanFactory.addBean("sink1", sink1);
			functionAdmin.createOrUpdateUserDefinedFunctions();
			assertThat(functionAdmin.getProcessedFunctions()).containsExactly(sink1);
		}

		@Test
		void sinkAndSourceProvided() {
			beanFactory.addBean("sink1", sink1);
			beanFactory.addBean("source1", source1);
			functionAdmin.createOrUpdateUserDefinedFunctions();
			assertThat(functionAdmin.getProcessedFunctions()).containsExactly(sink1, source1);
		}

		@Test
		void sourceProvided() {
			beanFactory.addBean("source1", source1);
			functionAdmin.createOrUpdateUserDefinedFunctions();
			assertThat(functionAdmin.getProcessedFunctions()).containsExactly(source1);
		}

	}

	@Nested
	class ProperCreateUpdateErrorHandling {

		@BeforeEach
		void provideFunctionsToBeanFactory() {
			beanFactory.addBean("function1", function1);
			beanFactory.addBean("sink1", sink1);
			beanFactory.addBean("source1", source1);
		}

		@Test
		void createAdminClientFails() throws PulsarClientException {
			when(springPulsarAdmin.createAdminClient()).thenThrow(new PulsarClientException("NOPE"));
			assertThatThrownBy(() -> functionAdmin.createOrUpdateUserDefinedFunctions())
					.isInstanceOf(PulsarException.class)
					.hasMessageContaining("Unable to create/update functions - could not create PulsarAdmin: NOPE");
		}

		@Nested
		class WithFailFast {

			@Test
			void firstProcessedFunctionFails() throws PulsarAdminException {
				var ex = new PulsarAdminException("BOOM");
				when(function1.functionExists(pulsarAdmin)).thenThrow(ex);
				var thrown = catchThrowableOfType(() -> functionAdmin.createOrUpdateUserDefinedFunctions(),
						PulsarFunctionException.class);
				assertThat(thrown.getFailures()).containsExactly(entry(function1, ex));
				verify(function1, never()).create(pulsarAdmin);
				verify(function1, never()).update(pulsarAdmin);
				verifyNoInteractions(sink1, source1);
				assertThat(functionAdmin.getProcessedFunctions()).isEmpty();
			}

			@Test
			void middleProcessedFunctionFails() throws PulsarAdminException {
				var ex = new PulsarAdminException("BOOM");
				when(sink1.functionExists(pulsarAdmin)).thenThrow(ex);
				var thrown = catchThrowableOfType(() -> functionAdmin.createOrUpdateUserDefinedFunctions(),
						PulsarFunctionException.class);
				assertThat(thrown.getFailures()).containsExactly(entry(sink1, ex));
				verify(function1).create(pulsarAdmin);
				verify(sink1, never()).create(pulsarAdmin);
				verify(sink1, never()).update(pulsarAdmin);
				verifyNoInteractions(source1);
				assertThat(functionAdmin.getProcessedFunctions()).containsExactly(function1);
			}

			@Test
			void lastProcessedFunctionFails() throws PulsarAdminException {
				var ex = new PulsarAdminException("BOOM");
				when(source1.functionExists(pulsarAdmin)).thenThrow(ex);
				var thrown = catchThrowableOfType(() -> functionAdmin.createOrUpdateUserDefinedFunctions(),
						PulsarFunctionException.class);
				assertThat(thrown.getFailures()).containsExactly(entry(source1, ex));
				verify(function1).create(pulsarAdmin);
				verify(sink1).create(pulsarAdmin);
				verify(source1, never()).create(pulsarAdmin);
				verify(source1, never()).update(pulsarAdmin);
				assertThat(functionAdmin.getProcessedFunctions()).containsExactly(function1, sink1);
			}

		}

		@Nested
		class WithoutFailFast {

			@BeforeEach
			void disableFailFastOnFunctionAdmin() {
				functionAdmin = new PulsarFunctionAdministration(springPulsarAdmin,
						beanFactory.getBeanProvider(PulsarFunction.class),
						beanFactory.getBeanProvider(PulsarSink.class), beanFactory.getBeanProvider(PulsarSource.class),
						false, true, true);
			}

			@Test
			void firstProcessedFunctionFails() throws PulsarAdminException {
				var ex = new PulsarAdminException("BOOM");
				when(function1.functionExists(pulsarAdmin)).thenThrow(ex);
				var thrown = catchThrowableOfType(() -> functionAdmin.createOrUpdateUserDefinedFunctions(),
						PulsarFunctionException.class);
				assertThat(thrown.getFailures()).containsExactly(entry(function1, ex));
				verify(function1, never()).create(pulsarAdmin);
				verify(function1, never()).update(pulsarAdmin);
				verify(sink1).create(pulsarAdmin);
				verify(source1).create(pulsarAdmin);
				assertThat(functionAdmin.getProcessedFunctions()).containsExactly(sink1, source1);
			}

			@Test
			void middleProcessedFunctionFails() throws PulsarAdminException {
				var ex = new PulsarAdminException("BOOM");
				when(sink1.functionExists(pulsarAdmin)).thenThrow(ex);
				var thrown = catchThrowableOfType(() -> functionAdmin.createOrUpdateUserDefinedFunctions(),
						PulsarFunctionException.class);
				assertThat(thrown.getFailures()).containsExactly(entry(sink1, ex));
				verify(function1).create(pulsarAdmin);
				verify(sink1, never()).create(pulsarAdmin);
				verify(sink1, never()).update(pulsarAdmin);
				verify(source1).create(pulsarAdmin);
				assertThat(functionAdmin.getProcessedFunctions()).containsExactly(function1, source1);
			}

			@Test
			void lastProcessedFunctionFails() throws PulsarAdminException {
				var ex = new PulsarAdminException("BOOM");
				when(source1.functionExists(pulsarAdmin)).thenThrow(ex);
				var thrown = catchThrowableOfType(() -> functionAdmin.createOrUpdateUserDefinedFunctions(),
						PulsarFunctionException.class);
				assertThat(thrown.getFailures()).containsExactly(entry(source1, ex));
				verify(function1).create(pulsarAdmin);
				verify(sink1).create(pulsarAdmin);
				verify(source1, never()).create(pulsarAdmin);
				verify(source1, never()).update(pulsarAdmin);
				assertThat(functionAdmin.getProcessedFunctions()).containsExactly(function1, sink1);
			}

			@Test
			void allProcessedFunctionsFail() throws PulsarAdminException {
				PulsarAdminException ex1 = new PulsarAdminException("BOOM1");
				PulsarAdminException ex2 = new PulsarAdminException("BOOM2");
				PulsarAdminException ex3 = new PulsarAdminException("BOOM3");
				when(function1.functionExists(pulsarAdmin)).thenThrow(ex1);
				when(sink1.functionExists(pulsarAdmin)).thenThrow(ex2);
				when(source1.functionExists(pulsarAdmin)).thenThrow(ex3);
				var thrown = catchThrowableOfType(() -> functionAdmin.createOrUpdateUserDefinedFunctions(),
						PulsarFunctionException.class);
				assertThat(thrown.getFailures()).containsExactly(entry(function1, ex1), entry(sink1, ex2),
						entry(source1, ex3));
				verify(function1, never()).create(pulsarAdmin);
				verify(function1, never()).update(pulsarAdmin);
				verify(sink1, never()).create(pulsarAdmin);
				verify(sink1, never()).update(pulsarAdmin);
				verify(source1, never()).create(pulsarAdmin);
				verify(source1, never()).update(pulsarAdmin);
				assertThat(functionAdmin.getProcessedFunctions()).isEmpty();
			}

		}

		@Nested
		class WithPropagationDisabled {

			@BeforeEach
			void disablePropagationOnFunctionAdmin() {
				functionAdmin = new PulsarFunctionAdministration(springPulsarAdmin,
						beanFactory.getBeanProvider(PulsarFunction.class),
						beanFactory.getBeanProvider(PulsarSink.class), beanFactory.getBeanProvider(PulsarSource.class),
						true, false, false);
			}

			@Test
			void createAdminClientFails() throws Exception {
				beanFactory.addBean("function1", function1);
				when(springPulsarAdmin.createAdminClient()).thenThrow(new PulsarClientException("NOPE"));
				var output = tapSystemErrAndOutNormalized(() -> functionAdmin.createOrUpdateUserDefinedFunctions());
				assertThat(output).contains("Unable to create/update functions - could not create PulsarAdmin: NOPE");
			}

			@Test
			void processedFunctionFails() throws Exception {
				beanFactory.addBean("function1", function1);
				when(function1.functionExists(pulsarAdmin)).thenThrow(new PulsarAdminException("BOOM"));
				var output = tapSystemErrAndOutNormalized(() -> functionAdmin.createOrUpdateUserDefinedFunctions());
				assertThat(output).contains("Encountered 1 error(s) creating/updating functions:",
						"PulsarAdminException: BOOM");
			}

		}

	}

	@Nested
	class ProperStopPolicyApiCalled {

		@Test
		void none() {
			var function = mockFunction("noneFunction");
			when(function.stopPolicy()).thenReturn(FunctionStopPolicy.NONE);
			functionAdmin.getProcessedFunctions().add(function);
			functionAdmin.enforceStopPolicyOnUserDefinedFunctions();
			verify(function).stopPolicy();
			verify(function, never()).stop(pulsarAdmin);
		}

		@Test
		void stop() {
			var function = mockFunction("stopFunction");
			when(function.stopPolicy()).thenReturn(FunctionStopPolicy.STOP);
			functionAdmin.getProcessedFunctions().add(function);
			functionAdmin.enforceStopPolicyOnUserDefinedFunctions();
			verify(function).stop(pulsarAdmin);
		}

		@Test
		void delete() {
			var function = mockFunction("deleteFunction");
			when(function.stopPolicy()).thenReturn(FunctionStopPolicy.DELETE);
			functionAdmin.getProcessedFunctions().add(function);
			functionAdmin.enforceStopPolicyOnUserDefinedFunctions();
			verify(function).delete(pulsarAdmin);
		}

		PulsarFunction mockFunction(String name) {
			var function = mock(PulsarFunction.class);
			when(function.name()).thenReturn(name);
			when(function.type()).thenReturn(FunctionType.FUNCTION);
			return function;
		}

	}

	@Nested
	class ProperStopPolicyProcessOrder {

		@BeforeEach
		void setStopPolicyOnFunctions() {
			when(function1.stopPolicy()).thenReturn(FunctionStopPolicy.STOP);
			when(sink1.stopPolicy()).thenReturn(FunctionStopPolicy.STOP);
			when(source1.stopPolicy()).thenReturn(FunctionStopPolicy.STOP);
		}

		@Test
		void noFunctionsProvided() throws PulsarClientException {
			functionAdmin.enforceStopPolicyOnUserDefinedFunctions();
			verify(springPulsarAdmin, never()).createAdminClient();
			verifyNoInteractions(pulsarAdmin);
		}

		@Test
		void processedInReverseOrder() {
			beanFactory.addBean("function1", function1);
			beanFactory.addBean("sink1", sink1);
			beanFactory.addBean("source1", source1);
			functionAdmin.getProcessedFunctions().add(function1);
			functionAdmin.getProcessedFunctions().add(sink1);
			functionAdmin.getProcessedFunctions().add(source1);
			functionAdmin.enforceStopPolicyOnUserDefinedFunctions();
			InOrder inOrder = inOrder(function1, sink1, source1);
			inOrder.verify(source1).stop(pulsarAdmin);
			inOrder.verify(sink1).stop(pulsarAdmin);
			inOrder.verify(function1).stop(pulsarAdmin);
		}

	}

	@Nested
	class ProperStopPolicyErrorHandling {

		@BeforeEach
		void setStopPolicyOnFunctionsAndAddToProcessedList() {
			when(function1.stopPolicy()).thenReturn(FunctionStopPolicy.STOP);
			when(sink1.stopPolicy()).thenReturn(FunctionStopPolicy.STOP);
			when(source1.stopPolicy()).thenReturn(FunctionStopPolicy.STOP);
			functionAdmin.getProcessedFunctions().add(function1);
			functionAdmin.getProcessedFunctions().add(sink1);
			functionAdmin.getProcessedFunctions().add(source1);
		}

		@Test
		void createAdminClientFails() throws PulsarClientException {
			when(springPulsarAdmin.createAdminClient()).thenThrow(new PulsarClientException("NOPE"));
			assertThatThrownBy(() -> functionAdmin.enforceStopPolicyOnUserDefinedFunctions())
					.isInstanceOf(PulsarException.class).hasMessageContaining(
							"Unable to enforce stop policy on functions - could not create PulsarAdmin: NOPE");
		}

		@Test
		void firstProcessedFunctionFails() {
			var ex = new PulsarException("BOOM");
			doThrow(ex).when(source1).stop(pulsarAdmin);
			var thrown = catchThrowableOfType(() -> functionAdmin.enforceStopPolicyOnUserDefinedFunctions(),
					PulsarFunctionException.class);
			assertThat(thrown.getFailures()).containsExactly(entry(source1, ex));
			verify(sink1).stop(pulsarAdmin);
			verify(function1).stop(pulsarAdmin);
		}

		@Test
		void middleProcessedFunctionFails() {
			var ex = new PulsarException("BOOM");
			doThrow(ex).when(sink1).stop(pulsarAdmin);
			var thrown = catchThrowableOfType(() -> functionAdmin.enforceStopPolicyOnUserDefinedFunctions(),
					PulsarFunctionException.class);
			assertThat(thrown.getFailures()).containsExactly(entry(sink1, ex));
			verify(source1).stop(pulsarAdmin);
			verify(function1).stop(pulsarAdmin);
		}

		@Test
		void lastProcessedFunctionFails() {
			var ex = new PulsarException("BOOM");
			doThrow(ex).when(function1).stop(pulsarAdmin);
			var thrown = catchThrowableOfType(() -> functionAdmin.enforceStopPolicyOnUserDefinedFunctions(),
					PulsarFunctionException.class);
			assertThat(thrown.getFailures()).containsExactly(entry(function1, ex));
			verify(source1).stop(pulsarAdmin);
			verify(sink1).stop(pulsarAdmin);
		}

		@Test
		void allProcessedFunctionsFail() {
			PulsarException ex1 = new PulsarException("BOOM1");
			PulsarException ex2 = new PulsarException("BOOM2");
			PulsarException ex3 = new PulsarException("BOOM3");
			doThrow(ex1).when(source1).stop(pulsarAdmin);
			doThrow(ex2).when(sink1).stop(pulsarAdmin);
			doThrow(ex3).when(function1).stop(pulsarAdmin);
			var thrown = catchThrowableOfType(() -> functionAdmin.enforceStopPolicyOnUserDefinedFunctions(),
					PulsarFunctionException.class);
			assertThat(thrown.getFailures()).containsExactly(entry(source1, ex1), entry(sink1, ex2),
					entry(function1, ex3));
		}

		@Nested
		class WithPropagationDisabled {

			@BeforeEach
			void disableStopPropagationOnFunctionAdmin() {
				functionAdmin = new PulsarFunctionAdministration(springPulsarAdmin,
						beanFactory.getBeanProvider(PulsarFunction.class),
						beanFactory.getBeanProvider(PulsarSink.class), beanFactory.getBeanProvider(PulsarSource.class),
						true, false, false);
			}

			@Test
			void createAdminClientFails() throws Exception {
				functionAdmin.getProcessedFunctions().add(function1);
				when(springPulsarAdmin.createAdminClient()).thenThrow(new PulsarClientException("NOPE"));
				var output = tapSystemErrAndOutNormalized(
						() -> functionAdmin.enforceStopPolicyOnUserDefinedFunctions());
				assertThat(output)
					.contains("Unable to enforce stop policy on functions - could not create PulsarAdmin: NOPE");
			}

			@Test
			void processedFunctionFails() throws Exception {
				functionAdmin.getProcessedFunctions().add(function1);
				doThrow(new PulsarException("BOOM")).when(function1).stop(pulsarAdmin);
				var output = tapSystemErrAndOutNormalized(
						() -> functionAdmin.enforceStopPolicyOnUserDefinedFunctions());
				assertThat(output).contains("Encountered 1 error(s) enforcing stop policy on functions:",
						"PulsarException: BOOM");
			}

		}

	}

}
