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

package org.springframework.pulsar.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.mockito.InOrder;

import org.springframework.pulsar.core.RestartableComponentSupport.State;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Tests for {@link RestartableSingletonFactory}.
 */
public class RestartableSingletonFactoryTests {

	@ParameterizedTest
	@EnumSource(value = State.class, mode = Mode.INCLUDE, names = { "CREATED", "STOPPED" })
	void startCreatesInstanceAndSetsStateToStarted(State initialFactoryState) {
		var fooFactory = spy(new FooFactory());
		setFactoryState(fooFactory, initialFactoryState);
		assertThat(fooFactory.getInstance()).isNull();
		fooFactory.start();
		assertThat(fooFactory.getInstance()).isInstanceOf(Foo.class);
		InOrder inOrder = inOrder(fooFactory);
		inOrder.verify(fooFactory).doStart();
		inOrder.verify(fooFactory).createInstance();
		verifyFactoryState(fooFactory, State.STARTED);
	}

	@ParameterizedTest
	@EnumSource(value = State.class, mode = Mode.EXCLUDE, names = { "CREATED", "STOPPED" })
	void startDoesNothingWhenStateIsInvalid(State initialFactoryState) {
		var fooFactory = spy(new FooFactory());
		setFactoryState(fooFactory, initialFactoryState);
		fooFactory.start();
		verify(fooFactory, never()).createInstance();
		verify(fooFactory, never()).doStart();
	}

	@ParameterizedTest
	@EnumSource(value = State.class, mode = Mode.INCLUDE, names = { "CREATED", "STARTED" })
	void stopStopsTheInstanceAndSetsStateToStopped(State initialFactoryState) {
		var fooFactory = spy(new FooFactory());
		fooFactory.start(); // creates instance we use to verify destroyInstamce
		var foo = fooFactory.getInstance();
		setFactoryState(fooFactory, initialFactoryState);
		fooFactory.stop();
		assertThat(fooFactory.getInstance()).isNull();
		InOrder inOrder = inOrder(fooFactory);
		inOrder.verify(fooFactory).doStop();
		inOrder.verify(fooFactory).stopInstance(eq(foo));
		verifyFactoryState(fooFactory, State.STOPPED);
	}

	@ParameterizedTest
	@EnumSource(value = State.class, mode = Mode.EXCLUDE, names = { "CREATED", "STARTED" })
	void stopDoesNothingWhenStateIsInvalid(State initialFactoryState) {
		var fooFactory = spy(new FooFactory());
		setFactoryState(fooFactory, initialFactoryState);
		fooFactory.stop();
		verify(fooFactory, never()).doStop();
		verify(fooFactory, never()).stopInstance(any(Foo.class));
	}

	@Test
	void stopDoesNotDiscardInstanceWhenDiscardInstanceReturnsFalse() {
		var fooFactory = new FooFactory() {
			@Override
			protected boolean discardInstanceAfterStop() {
				return false;
			}
		};
		fooFactory.start();
		fooFactory.stop();
		assertThat(fooFactory.getInstance()).isNotNull();
	}

	@Test
	void destroyCallsStopAndSetsStateToDestroyed() {
		var fooFactory = spy(new FooFactory());
		fooFactory.start();
		fooFactory.destroy();
		verify(fooFactory).stop();
		verifyFactoryState(fooFactory, State.DESTROYED);
	}

	@Test
	void initializationCreatesInstance() throws Exception {
		var fooFactory = spy(new FooFactory());
		fooFactory.afterPropertiesSet();
		verify(fooFactory).createInstance();
	}

	@Test
	void createInstanceNotCalledWhenInstanceSetInConstructor() throws Exception {
		var foo = new Foo("the-one");
		var fooFactory = spy(new FooFactory(foo));
		fooFactory.afterPropertiesSet();
		fooFactory.start();
		verify(fooFactory, never()).createInstance();
		assertThat(fooFactory.getInstance()).isEqualTo(foo);
	}

	@Test
	void createInstanceOnlyCalledOnceDuringStartup() throws Exception {
		var fooFactory = spy(new FooFactory());
		fooFactory.afterPropertiesSet();
		fooFactory.start();
		verify(fooFactory).createInstance();
	}

	void setFactoryState(RestartableSingletonFactory<?> factory, State factoryState) {
		ReflectionTestUtils.setField(factory, "state", new AtomicReference<>(factoryState));
		verifyFactoryState(factory, factoryState);
	}

	void verifyFactoryState(RestartableSingletonFactory<?> factory, State expectedState) {
		assertThat(factory).extracting("state").satisfies((state) -> {
			assertThat(state.toString()).contains(expectedState.name());
		});
	}

	static class FooFactory extends RestartableSingletonFactory<Foo> {

		FooFactory() {
			super();
		}

		FooFactory(Foo instance) {
			super(instance);
		}

		@Override
		protected Foo createInstance() {
			return new Foo("restart:" + System.currentTimeMillis());
		}

	}

	static class FooKeepsInstanceFactory extends FooFactory {

		@Override
		protected boolean discardInstanceAfterStop() {
			return false;
		}

	}

	record Foo(String name) {

	}

}
