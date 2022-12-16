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

package org.springframework.pulsar.spring.cloud.stream.binder.provisioning;

import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.pulsar.core.PulsarTopic;
import org.springframework.pulsar.spring.cloud.stream.binder.properties.PulsarConsumerProperties;
import org.springframework.pulsar.spring.cloud.stream.binder.properties.PulsarProducerProperties;

/**
 * Pulsar topic provisioner.
 *
 * @author Soby Chacko
 */
public class PulsarTopicProvisioner implements
		ProvisioningProvider<ExtendedConsumerProperties<PulsarConsumerProperties>, ExtendedProducerProperties<PulsarProducerProperties>> {

	// TODO: Retrieve partitions through config
	@Override
	public ProducerDestination provisionProducerDestination(String name,
			ExtendedProducerProperties<PulsarProducerProperties> properties) throws ProvisioningException {
		PulsarTopic pulsarTopic = PulsarTopic.builder(name).numberOfPartitions(1).build();
		return new PulsarProducerDestination(pulsarTopic.topicName(), pulsarTopic.numberOfPartitions());
	}

	@Override
	public ConsumerDestination provisionConsumerDestination(String name, String group,
			ExtendedConsumerProperties<PulsarConsumerProperties> properties) throws ProvisioningException {
		PulsarTopic pulsarTopic = PulsarTopic.builder(name).numberOfPartitions(1).build();
		return new PulsarConsumerDestination(pulsarTopic.topicName(), pulsarTopic.numberOfPartitions());
	}

	private static class PulsarDestination implements ProducerDestination, ConsumerDestination {

		private final String producerDestinationName;

		private final int partitions;

		PulsarDestination(String destinationName, Integer partitions) {
			this.producerDestinationName = destinationName;
			this.partitions = partitions;
		}

		@Override
		public String getName() {
			return this.producerDestinationName;
		}

		@Override
		public String getNameForPartition(int partition) {
			return this.producerDestinationName;
		}

		public int getPartitions() {
			return this.partitions;
		}

	}

	private static class PulsarProducerDestination extends PulsarDestination {

		PulsarProducerDestination(String destinationName, Integer partitions) {
			super(destinationName, partitions);
		}

		@Override
		public String toString() {
			return "PulsarProducerDestination{" + "producerDestinationName='" + getName() + '\'' + ", partitions="
					+ getPartitions() + '}';
		}

	}

	private static class PulsarConsumerDestination extends PulsarDestination {

		PulsarConsumerDestination(String destinationName, Integer partitions) {
			super(destinationName, partitions);
		}

		@Override
		public String toString() {
			return "PulsarConsumerDestination{" + "producerDestinationName='" + getName() + '\'' + ", partitions="
					+ getPartitions() + '}';
		}

	}

}
