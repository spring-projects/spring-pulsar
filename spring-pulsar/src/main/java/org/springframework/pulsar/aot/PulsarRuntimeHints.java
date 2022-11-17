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

package org.springframework.pulsar.aot;

import java.util.HashSet;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.apache.pulsar.client.admin.internal.OffloadProcessStatusImpl;
import org.apache.pulsar.client.admin.internal.PulsarAdminBuilderImpl;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.util.SecretsSerializer;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.shade.io.netty.buffer.AbstractByteBufAllocator;
import org.apache.pulsar.shade.io.netty.channel.socket.nio.NioDatagramChannel;
import org.apache.pulsar.shade.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.pulsar.shade.io.netty.util.ReferenceCountUtil;

import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.ReflectionHints;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;
import org.springframework.lang.Nullable;

/**
 * {@link RuntimeHintsRegistrar} for Spring for Apache Pulsar.
 *
 * @author Soby Chacko
 */
public class PulsarRuntimeHints implements RuntimeHintsRegistrar {

	@Override
	public void registerHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {
		ReflectionHints reflectionHints = hints.reflection();
		// The following components need access to declared constructors, invoke declared
		// methods
		// and introspect all public methods. The components are a mix of JDK classes,
		// core Pulsar classes,
		// some other shaded components available through Pulsar client.
		Stream.of(HashSet.class, TreeMap.class, Authentication.class, AuthenticationDataProvider.class,
				SecretsSerializer.class, NioSocketChannel.class, AbstractByteBufAllocator.class,
				NioDatagramChannel.class, PulsarAdminBuilderImpl.class, OffloadProcessStatusImpl.class, Commands.class,
				ReferenceCountUtil.class).forEach(
						type -> reflectionHints.registerType(type,
								builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
										MemberCategory.INVOKE_DECLARED_METHODS,
										MemberCategory.INTROSPECT_PUBLIC_METHODS)));

		// In addition to the above member category levels, these components need field
		// and declared class level access.
		Stream.of(ClientConfigurationData.class, ConsumerConfigurationData.class, ProducerConfigurationData.class)
				.forEach(type -> reflectionHints.registerType(type,
						builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
								MemberCategory.INVOKE_DECLARED_METHODS, MemberCategory.INTROSPECT_PUBLIC_METHODS,
								MemberCategory.DECLARED_CLASSES, MemberCategory.DECLARED_FIELDS)));

		// These are inaccessible interfaces/classes in a normal scenario, thus using the
		// String version,
		// and we need field level access in them.
		Stream.of(
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.BaseMpscLinkedArrayQueueProducerFields",
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.BaseMpscLinkedArrayQueueConsumerFields",
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.BaseMpscLinkedArrayQueueColdProducerFields",
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueueProducerIndexField",
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueueProducerLimitField",
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueueConsumerIndexField")
				.forEach(typeName -> reflectionHints.registerTypeIfPresent(classLoader, typeName,
						MemberCategory.DECLARED_FIELDS));

		Stream.of("reactor.core.publisher.Flux", "com.github.benmanes.caffeine.cache.SSMSA",
				"com.github.benmanes.caffeine.cache.PSAMS")
				.forEach(typeName -> reflectionHints.registerTypeIfPresent(classLoader, typeName,
						MemberCategory.INVOKE_DECLARED_CONSTRUCTORS, MemberCategory.INVOKE_DECLARED_METHODS,
						MemberCategory.INTROSPECT_PUBLIC_METHODS));

		// Registering JDK dynamic proxies for these interfaces. Since the Connection
		// interface is protected,
		// wee need to use the string version of proxy registration. Although the other
		// interfaces are public,
		// due to ConnectionHandler$Connection being protected forces all of them to be
		// registered using the
		// string version of the API because all of them need to be registered through a
		// single call.
		hints.proxies().registerJdkProxy(TypeReference.of("org.apache.pulsar.shade.io.netty.util.TimerTask"),
				TypeReference.of("org.apache.pulsar.client.impl.ConnectionHandler$Connection"),
				TypeReference.of("org.apache.pulsar.client.api.Producer"),
				TypeReference.of("org.springframework.aop.SpringProxy"),
				TypeReference.of("org.springframework.aop.framework.Advised"),
				TypeReference.of("org.springframework.core.DecoratingProxy"));
	}

}
