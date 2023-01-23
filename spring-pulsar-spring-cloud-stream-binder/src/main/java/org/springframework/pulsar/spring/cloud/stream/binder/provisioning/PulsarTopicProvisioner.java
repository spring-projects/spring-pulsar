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

package org.springframework.pulsar.spring.cloud.stream.binder.provisioning;

import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.pulsar.core.PulsarAdministration;
import org.springframework.pulsar.core.PulsarTopic;
import org.springframework.pulsar.spring.cloud.stream.binder.properties.PulsarBinderConfigurationProperties;
import org.springframework.pulsar.spring.cloud.stream.binder.properties.PulsarConsumerProperties;
import org.springframework.pulsar.spring.cloud.stream.binder.properties.PulsarProducerProperties;

/**
 * Pulsar topic provisioner.
 *
 * @author Soby Chacko
 */
public class PulsarTopicProvisioner implements
		ProvisioningProvider<ExtendedConsumerProperties<PulsarConsumerProperties>, ExtendedProducerProperties<PulsarProducerProperties>> {

	private final PulsarAdministration pulsarAdministration;

	private final PulsarBinderConfigurationProperties pulsarBinderConfigurationProperties;

	public PulsarTopicProvisioner(PulsarAdministration pulsarAdministration,
			PulsarBinderConfigurationProperties pulsarBinderConfigurationProperties) {
		this.pulsarAdministration = pulsarAdministration;
		this.pulsarBinderConfigurationProperties = pulsarBinderConfigurationProperties;
	}

	@Override
	public ProducerDestination provisionProducerDestination(String name,
			ExtendedProducerProperties<PulsarProducerProperties> pulsarProducerProperties)
			throws ProvisioningException {

		int partitionCount = this.pulsarBinderConfigurationProperties.partitionCount();
		int partitionCountOnBinding = pulsarProducerProperties.getPartitionCount();
		if (partitionCountOnBinding > 1) {
			partitionCount = partitionCountOnBinding;
		}
		PulsarTopic pulsarTopic = PulsarTopic.builder(name).numberOfPartitions(partitionCount).build();
		// this.pulsarAdministration.createOrModifyTopics(pulsarTopic);
		return new PulsarDestination(pulsarTopic.topicName(), pulsarTopic.numberOfPartitions());
	}

	@Override
	public ConsumerDestination provisionConsumerDestination(String name, String group,
			ExtendedConsumerProperties<PulsarConsumerProperties> pulsarConsumerProperties)
			throws ProvisioningException {
		int partitionCount = this.pulsarBinderConfigurationProperties.partitionCount();

		int partitionCountOnBinding = pulsarConsumerProperties.getExtension().getPartitionCount();
		if (partitionCountOnBinding > 1) {
			partitionCount = partitionCountOnBinding;
		}
		PulsarTopic pulsarTopic = PulsarTopic.builder(name).numberOfPartitions(partitionCount).build();
		// this.pulsarAdministration.createOrModifyTopics(pulsarTopic);
		return new PulsarDestination(pulsarTopic.topicName(), pulsarTopic.numberOfPartitions());
	}

	private record PulsarDestination(String destinationName,
			Integer partitions) implements ProducerDestination, ConsumerDestination {

		@Override
		public String getName() {
			return this.destinationName;
		}

		@Override
		public String getNameForPartition(int partition) {
			return this.destinationName;
		}
	}

}
