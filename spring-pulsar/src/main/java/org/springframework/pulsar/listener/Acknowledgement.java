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

package org.springframework.pulsar.listener;

import java.util.List;

import org.apache.pulsar.client.api.MessageId;

/**
 * Contract for manual acknowledgment.
 *
 * When manual acknowledgment is used, applications can inject an Acknowledgment object in
 * the listener and then invoke manual acknowledgment.
 *
 * @author Soby Chacko
 */
public interface Acknowledgement {

	/**
	 * Manually acknowledges the current message that the listener is operating upon.
	 */
	void acknowledge();

	/**
	 * Manually acknowledges by the message id.
	 * @param messageId message id.
	 */
	void acknowledge(MessageId messageId);

	/**
	 * Manually acknowledges a list of messages based on their message id's.
	 * @param messageIds collection of message id's.
	 */
	void acknowledge(List<MessageId> messageIds);

	/**
	 * Negatively acknowledges the current message manually.
	 */
	void nack();

	/**
	 * Negative acknowledges the current message based on the message id.
	 * @param messageId message id.
	 */
	void nack(MessageId messageId);

}
