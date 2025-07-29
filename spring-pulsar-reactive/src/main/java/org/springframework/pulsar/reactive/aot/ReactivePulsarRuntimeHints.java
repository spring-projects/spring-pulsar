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

package org.springframework.pulsar.reactive.aot;

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
import org.jspecify.annotations.Nullable;

import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.ReflectionHints;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

/**
 * {@link RuntimeHintsRegistrar} for Spring for Apache Pulsar (reactive).
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public class ReactivePulsarRuntimeHints implements RuntimeHintsRegistrar {

	@Override
	public void registerHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {
		ReflectionHints reflectionHints = hints.reflection();
		// The following components need access to declared constructors, invoke declared
		// methods
		// and introspect all public methods. The components are a mix of JDK classes,
		// core Pulsar classes,
		// some other shaded components available through Pulsar client.
		Stream
			.of(HashSet.class, TreeMap.class, Authentication.class, AuthenticationDataProvider.class,
					SecretsSerializer.class, NioSocketChannel.class, AbstractByteBufAllocator.class,
					NioDatagramChannel.class, PulsarAdminBuilderImpl.class, OffloadProcessStatusImpl.class,
					Commands.class, ReferenceCountUtil.class)
			.forEach(type -> reflectionHints.registerType(type, builder -> builder
				.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS, MemberCategory.INVOKE_DECLARED_METHODS)));

		// In addition to the above member category levels, these components need field
		// and declared class level access.
		Stream.of(ClientConfigurationData.class, ConsumerConfigurationData.class, ProducerConfigurationData.class)
			.forEach(type -> reflectionHints.registerType(type,
					builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
							MemberCategory.INVOKE_DECLARED_METHODS, MemberCategory.ACCESS_DECLARED_FIELDS)));

		// These are inaccessible interfaces/classes in a normal scenario, thus using the
		// String version, and we need field level access in them.
		Stream.of(
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.BaseMpscLinkedArrayQueueProducerFields",
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.BaseMpscLinkedArrayQueueConsumerFields",
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.BaseMpscLinkedArrayQueueColdProducerFields",
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueueConsumerIndexField",
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueueProducerIndexField",
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueueProducerLimitField",
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.unpadded.MpscUnpaddedArrayQueueConsumerIndexField",
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.unpadded.MpscUnpaddedArrayQueueProducerIndexField",
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.unpadded.MpscUnpaddedArrayQueueProducerLimitField")
			.forEach(typeName -> reflectionHints.registerTypeIfPresent(classLoader, typeName,
					MemberCategory.ACCESS_DECLARED_FIELDS));

		// @formatter:off
		Stream.of(
		"reactor.core.publisher.Flux",
				"java.lang.Thread",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.BBHeader$ReadAndWriteCounterRef",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.BBHeader$ReadCounterRef",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.BLCHeader",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.BLCHeader$DrainStatusRef",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.BLCHeader$PadDrainStatus",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.BaseMpscLinkedArrayQueueColdProducerFields",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.BaseMpscLinkedArrayQueueConsumerFields",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.BaseMpscLinkedArrayQueueProducerFields",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.BoundedLocalCache",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.CacheLoader",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.FS",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.FW",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.PD",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.PS",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.PSA",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.PSAMS",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.PSAW",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.PSAWMW",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.PSMS",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.PSMW",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.PSR",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.PSRMS",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.PSW",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.PSWMS",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.PSWMW",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.PW",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.SI",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.SS",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.SSA",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.SSAW",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.SSL",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.SSLAW",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.SSLMS",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.SSLMSA",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.SSLMSAW",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.SSLSW",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.SSLW",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.SSMS",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.SSMSA",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.SSMSAW",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.SSMSR",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.SSMSW",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.SSMW",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.SSSMS",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.SSSMWW",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.SSSW",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.SSW",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.StripedBuffer",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.UnboundedLocalCache",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.WI",
				"org.apache.pulsar.reactive.shade.com.github.benmanes.caffeine.cache.WS")
			.forEach(type -> reflectionHints.registerTypeIfPresent(classLoader, type,
					builder -> builder.withMembers(
							MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
							MemberCategory.INVOKE_PUBLIC_METHODS,
							MemberCategory.INVOKE_DECLARED_METHODS,
							MemberCategory.ACCESS_DECLARED_FIELDS)));
		var threadLocalRandomProbeField = ReflectionUtils.findField(Thread.class, "threadLocalRandomProbe");
		Assert.notNull(threadLocalRandomProbeField, "threadLocalRandomProbe not found on Thread.class");
		reflectionHints.registerField(threadLocalRandomProbeField);

		// @formatter:on

		// Registering JDK dynamic proxies for these interfaces. Since the Connection
		// interface is protected,
		// wee need to use the string version of proxy registration. Although the other
		// interfaces are public,
		// due to ConnectionHandler$Connection being protected forces all of them to be
		// registered using the
		// string version of the API because all of them need to be registered through a
		// single call.
		hints.proxies()
			.registerJdkProxy(TypeReference.of("org.apache.pulsar.shade.io.netty.util.TimerTask"),
					TypeReference.of("org.apache.pulsar.client.impl.ConnectionHandler$Connection"),
					TypeReference.of("org.apache.pulsar.client.api.Producer"),
					TypeReference.of("org.springframework.aop.SpringProxy"),
					TypeReference.of("org.springframework.aop.framework.Advised"),
					TypeReference.of("org.springframework.core.DecoratingProxy"));
	}

}
