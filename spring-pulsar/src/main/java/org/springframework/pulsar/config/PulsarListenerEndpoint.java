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

package org.springframework.pulsar.config;

import java.util.Properties;

import org.springframework.pulsar.listener.AckMode;
import org.springframework.pulsar.listener.PulsarMessageListenerContainer;

/**
 * Model for a Pulsar listener endpoint. Can be used against a
 * {@link org.springframework.pulsar.annotation.PulsarListenerConfigurer} to register
 * endpoints programmatically.
 *
 * @author Soby Chacko
 * @author Alexander Preuß
 */
public interface PulsarListenerEndpoint extends ListenerEndpoint<PulsarMessageListenerContainer> {

	boolean isBatchListener();

	Properties getConsumerProperties();

	AckMode getAckMode();

}
