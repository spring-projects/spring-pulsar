/*
 * Copyright 2022-2023 the original author or authors.
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
import java.util.LinkedHashMap;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.apache.pulsar.client.admin.ListTopicsOptions;
import org.apache.pulsar.client.admin.internal.OffloadProcessStatusImpl;
import org.apache.pulsar.client.admin.internal.PulsarAdminBuilderImpl;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.util.SecretsSerializer;
import org.apache.pulsar.common.protocol.Commands;
import org.jspecify.annotations.Nullable;

import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.ReflectionHints;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;

/**
 * {@link RuntimeHintsRegistrar} for Spring for Apache Pulsar.
 *
 * @author Soby Chacko
 * @author Chris Bono
 */
public class PulsarRuntimeHints implements RuntimeHintsRegistrar {

	@Override
	public void registerHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {
		ReflectionHints reflectionHints = hints.reflection();
		// The following components need access to declared constructors, invoke declared
		// methods and introspect all public methods. The components are a mix of JDK
		// classes, core Pulsar classes, and some other shaded components available
		// through Pulsar client.
		Stream
			.of(HashSet.class, LinkedHashMap.class, TreeMap.class, Authentication.class,
					AuthenticationDataProvider.class, SecretsSerializer.class, PulsarAdminBuilderImpl.class,
					OffloadProcessStatusImpl.class, Commands.class)
			.forEach(type -> reflectionHints.registerType(type, builder -> builder
				.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS, MemberCategory.INVOKE_DECLARED_METHODS)));

		// In addition to the above member category levels, these components need field
		// and declared class level access.
		Stream
			.of(ClientConfigurationData.class, ConsumerConfigurationData.class, ProducerConfigurationData.class,
					ListTopicsOptions.class)
			.forEach(type -> reflectionHints.registerType(type,
					builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
							MemberCategory.INVOKE_DECLARED_METHODS, MemberCategory.ACCESS_DECLARED_FIELDS)));

		// @formatter:off
		// These are shaded classes and other inaccessible interfaces/classes (thus using
		// string version of API).
		Stream.of("org.apache.pulsar.client.admin.internal.JacksonConfigurator",
				"org.apache.pulsar.client.impl.conf.TopicConsumerConfigurationData",
				"org.apache.pulsar.client.impl.conf.TopicConsumerConfigurationData$TopicNameMatcher",
				"org.apache.pulsar.client.util.SecretsSerializer",
				"org.apache.pulsar.common.classification.InterfaceAudience$Public",
				"org.apache.pulsar.common.protocol.ByteBufPair$Encoder",
				"org.apache.pulsar.common.protocol.PulsarDecoder",
				"org.apache.pulsar.common.util.netty.DnsResolverUtil",
				"org.apache.pulsar.shade.com.fasterxml.jackson.databind.ext.Java7HandlersImpl",
				"org.apache.pulsar.shade.com.fasterxml.jackson.databind.ext.Java7SupportImpl",
				"org.apache.pulsar.shade.com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector",
				"org.apache.pulsar.shade.com.fasterxml.jackson.module.jaxb.deser.DataHandlerJsonDeserializer",
				"org.apache.pulsar.shade.com.fasterxml.jackson.module.jaxb.ser.DataHandlerJsonSerializer",
				"org.apache.pulsar.shade.com.google.common.util.concurrent.AbstractFuture",
				"org.apache.pulsar.shade.com.google.common.util.concurrent.AbstractFuture$Waiter",
				"org.apache.pulsar.shade.io.netty.buffer.AbstractByteBufAllocator",
				"org.apache.pulsar.shade.io.netty.buffer.AbstractReferenceCountedByteBuf",
				"org.apache.pulsar.shade.io.netty.channel.ChannelDuplexHandler",
				"org.apache.pulsar.shade.io.netty.channel.ChannelHandlerAdapter",
				"org.apache.pulsar.shade.io.netty.channel.ChannelInboundHandlerAdapter",
				"org.apache.pulsar.shade.io.netty.channel.ChannelInitializer",
				"org.apache.pulsar.shade.io.netty.channel.ChannelOutboundHandlerAdapter",
				"org.apache.pulsar.shade.io.netty.channel.CombinedChannelDuplexHandler",
				"org.apache.pulsar.shade.io.netty.channel.DefaultChannelPipeline$HeadContext",
				"org.apache.pulsar.shade.io.netty.channel.DefaultChannelPipeline$TailContext",
				"org.apache.pulsar.shade.io.netty.channel.DefaultFileRegion",
				"org.apache.pulsar.shade.io.netty.channel.epoll.NativeDatagramPacketArray$NativeDatagramPacket",
				"org.apache.pulsar.shade.io.netty.channel.socket.nio.NioDatagramChannel",
				"org.apache.pulsar.shade.io.netty.channel.socket.nio.NioSocketChannel",
				"org.apache.pulsar.shade.io.netty.channel.unix.PeerCredentials",
				"org.apache.pulsar.shade.io.netty.handler.codec.ByteToMessageDecoder",
				"org.apache.pulsar.shade.io.netty.handler.codec.LengthFieldBasedFrameDecoder",
				"org.apache.pulsar.shade.io.netty.handler.codec.MessageToMessageEncoder",
				"org.apache.pulsar.shade.io.netty.handler.codec.dns.DatagramDnsQueryEncoder",
				"org.apache.pulsar.shade.io.netty.handler.codec.http.HttpClientCodec",
				"org.apache.pulsar.shade.io.netty.handler.codec.http.HttpContentDecoder",
				"org.apache.pulsar.shade.io.netty.handler.codec.http.HttpContentDecompressor",
				"org.apache.pulsar.shade.io.netty.handler.flush.FlushConsolidationHandler",
				"org.apache.pulsar.shade.io.netty.handler.stream.ChunkedWriteHandler",
				"org.apache.pulsar.shade.io.netty.resolver.dns.DnsNameResolver$1",
				"org.apache.pulsar.shade.io.netty.resolver.dns.DnsNameResolver$3",
				"org.apache.pulsar.shade.io.netty.resolver.dns.DnsNameResolver$DnsResponseHandler",
				"org.apache.pulsar.shade.io.netty.util.AbstractReferenceCounted",
				"org.apache.pulsar.shade.io.netty.util.ReferenceCountUtil",
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.BaseMpscLinkedArrayQueueColdProducerFields",
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.BaseMpscLinkedArrayQueueConsumerFields",
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.BaseMpscLinkedArrayQueueProducerFields",
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueueConsumerIndexField",
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueueProducerIndexField",
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.MpscArrayQueueProducerLimitField",
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.unpadded.MpscUnpaddedArrayQueueConsumerIndexField",
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.unpadded.MpscUnpaddedArrayQueueProducerIndexField",
				"org.apache.pulsar.shade.io.netty.util.internal.shaded.org.jctools.queues.unpadded.MpscUnpaddedArrayQueueProducerLimitField",
				"org.apache.pulsar.shade.javax.inject.Named",
				"org.apache.pulsar.shade.javax.inject.Singleton",
				"org.apache.pulsar.shade.org.asynchttpclient.config.AsyncHttpClientConfigDefaults",
				"org.apache.pulsar.shade.org.asynchttpclient.config.AsyncHttpClientConfigHelper",
				"org.apache.pulsar.shade.org.asynchttpclient.netty.channel.ChannelManager$1",
				"org.apache.pulsar.shade.org.asynchttpclient.netty.handler.AsyncHttpClientHandler",
				"org.apache.pulsar.shade.org.asynchttpclient.netty.handler.HttpHandler",
				"org.apache.pulsar.shade.org.glassfish.hk2.api.DynamicConfigurationService",
				"org.apache.pulsar.shade.org.glassfish.hk2.internal.PerThreadContext",
				"org.apache.pulsar.shade.org.glassfish.hk2.internal.PerThreadContext",
				"org.apache.pulsar.shade.org.glassfish.hk2.utilities.ServiceLocatorUtilities",
				"org.apache.pulsar.shade.org.glassfish.jersey.client.ChunkedInputReader",
				"org.apache.pulsar.shade.org.glassfish.jersey.client.ClientAsyncExecutor",
				"org.apache.pulsar.shade.org.glassfish.jersey.client.ClientBackgroundScheduler",
				"org.apache.pulsar.shade.org.glassfish.jersey.client.DefaultClientAsyncExecutorProvider",
				"org.apache.pulsar.shade.org.glassfish.jersey.client.DefaultClientBackgroundSchedulerProvider",
				"org.apache.pulsar.shade.org.glassfish.jersey.client.JerseyClientBuilder",
				"org.apache.pulsar.shade.org.glassfish.jersey.client.JerseyClientBuilder",
				"org.apache.pulsar.shade.org.glassfish.jersey.inject.hk2.ContextInjectionResolverImpl",
				"org.apache.pulsar.shade.org.glassfish.jersey.inject.hk2.Hk2InjectionManagerFactory",
				"org.apache.pulsar.shade.org.glassfish.jersey.inject.hk2.Hk2RequestScope",
				"org.apache.pulsar.shade.org.glassfish.jersey.inject.hk2.InstanceSupplierFactoryBridge",
				"org.apache.pulsar.shade.org.glassfish.jersey.inject.hk2.JerseyErrorService",
				"org.apache.pulsar.shade.org.glassfish.jersey.inject.hk2.RequestContext",
				"org.apache.pulsar.shade.org.glassfish.jersey.internal.JaxrsProviders",
				"org.apache.pulsar.shade.org.glassfish.jersey.internal.RuntimeDelegateImpl",
				"org.apache.pulsar.shade.org.glassfish.jersey.internal.config.ExternalPropertiesAutoDiscoverable",
				"org.apache.pulsar.shade.org.glassfish.jersey.internal.config.ExternalPropertiesConfigurationFeature",
				"org.apache.pulsar.shade.org.glassfish.jersey.internal.inject.Custom",
				"org.apache.pulsar.shade.org.glassfish.jersey.jackson.JacksonFeature",
				"org.apache.pulsar.shade.org.glassfish.jersey.jackson.internal.DefaultJacksonJaxbJsonProvider",
				"org.apache.pulsar.shade.org.glassfish.jersey.jackson.internal.JacksonAutoDiscoverable",
				"org.apache.pulsar.shade.org.glassfish.jersey.jackson.internal.jackson.jaxrs.base.ProviderBase",
				"org.apache.pulsar.shade.org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJaxbJsonProvider",
				"org.apache.pulsar.shade.org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJsonProvider",
				"org.apache.pulsar.shade.org.glassfish.jersey.logging.LoggingFeatureAutoDiscoverable",
				"org.apache.pulsar.shade.org.glassfish.jersey.media.multipart.MultiPartFeature",
				"org.apache.pulsar.shade.org.glassfish.jersey.media.multipart.internal.MultiPartReaderClientSide",
				"org.apache.pulsar.shade.org.glassfish.jersey.media.multipart.internal.MultiPartWriter",
				"org.apache.pulsar.shade.org.glassfish.jersey.message.internal.AbstractFormProvider",
				"org.apache.pulsar.shade.org.glassfish.jersey.message.internal.AbstractMessageReaderWriterProvider",
				"org.apache.pulsar.shade.org.glassfish.jersey.message.internal.BasicTypesMessageProvider",
				"org.apache.pulsar.shade.org.glassfish.jersey.message.internal.ByteArrayProvider",
				"org.apache.pulsar.shade.org.glassfish.jersey.message.internal.DataSourceProvider",
				"org.apache.pulsar.shade.org.glassfish.jersey.message.internal.EnumMessageProvider",
				"org.apache.pulsar.shade.org.glassfish.jersey.message.internal.FileProvider",
				"org.apache.pulsar.shade.org.glassfish.jersey.message.internal.FormMultivaluedMapProvider",
				"org.apache.pulsar.shade.org.glassfish.jersey.message.internal.FormProvider",
				"org.apache.pulsar.shade.org.glassfish.jersey.message.internal.InputStreamProvider",
				"org.apache.pulsar.shade.org.glassfish.jersey.message.internal.ReaderProvider",
				"org.apache.pulsar.shade.org.glassfish.jersey.message.internal.RenderedImageProvider",
				"org.apache.pulsar.shade.org.glassfish.jersey.message.internal.SourceProvider",
				"org.apache.pulsar.shade.org.glassfish.jersey.message.internal.SourceProvider$DomSourceReader",
				"org.apache.pulsar.shade.org.glassfish.jersey.message.internal.SourceProvider$SaxSourceReader",
				"org.apache.pulsar.shade.org.glassfish.jersey.message.internal.SourceProvider$SourceWriter",
				"org.apache.pulsar.shade.org.glassfish.jersey.message.internal.SourceProvider$StreamSourceReader",
				"org.apache.pulsar.shade.org.glassfish.jersey.message.internal.StreamingOutputProvider",
				"org.apache.pulsar.shade.org.glassfish.jersey.message.internal.StringMessageProvider",
				"org.apache.pulsar.shade.org.glassfish.jersey.process.internal.RequestScope",
				"org.apache.pulsar.shade.org.glassfish.jersey.spi.AbstractThreadPoolProvider",
				"org.apache.pulsar.shade.org.glassfish.jersey.spi.ScheduledThreadPoolExecutorProvider",
				"org.apache.pulsar.shade.org.glassfish.jersey.spi.ThreadPoolExecutorProvider",
				"org.apache.pulsar.shade.org.jvnet.hk2.internal.DynamicConfigurationServiceImpl",
				"org.apache.pulsar.shade.org.jvnet.hk2.internal.ServiceLocatorRuntimeImpl",
				"org.springframework.pulsar.shade.com.github.benmanes.caffeine.cache.PSAMS",
				"org.springframework.pulsar.shade.com.github.benmanes.caffeine.cache.PSW",
				"org.springframework.pulsar.shade.com.github.benmanes.caffeine.cache.PSWMS",
				"org.springframework.pulsar.shade.com.github.benmanes.caffeine.cache.SSLA",
				"org.springframework.pulsar.shade.com.github.benmanes.caffeine.cache.SSMSA",
				"org.springframework.pulsar.shade.com.github.benmanes.caffeine.cache.SSLMSAW")
			.forEach(type -> reflectionHints.registerTypeIfPresent(classLoader, type,
					builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
							MemberCategory.INVOKE_PUBLIC_METHODS, MemberCategory.INVOKE_DECLARED_METHODS,
							MemberCategory.ACCESS_DECLARED_FIELDS)));
		// @formatter:on

		// Registering JDK dynamic proxies for these interfaces. Since the Connection
		// interface is protected, we need to use the string version of proxy
		// registration.
		// Although the other interfaces are public, due to ConnectionHandler$Connection
		// being protected forces all of them to be registered using the string version
		// of the API because all of them need to be registered through a single call.
		hints.proxies()
			.registerJdkProxy(TypeReference.of("org.apache.pulsar.shade.io.netty.util.TimerTask"),
					TypeReference.of("org.apache.pulsar.client.impl.ConnectionHandler$Connection"),
					TypeReference.of("org.apache.pulsar.client.api.Producer"),
					TypeReference.of("org.springframework.aop.SpringProxy"),
					TypeReference.of("org.springframework.aop.framework.Advised"),
					TypeReference.of("org.springframework.core.DecoratingProxy"));

		// Register required properties files
		hints.resources()
			.registerPatternIfPresent(classLoader, "org/apache/pulsar/shade/org/asynchttpclient/config/",
					builder -> builder.includes("*.properties"));
	}

}
