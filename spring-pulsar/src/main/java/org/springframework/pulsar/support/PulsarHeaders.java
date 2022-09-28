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

package org.springframework.pulsar.support;

/**
 * Pulsar specific message headers.
 *
 * @author Soby Chacko
 */
public abstract class PulsarHeaders {

	/**
	 * The prefix for Pulsar headers.
	 */
	public static final String PREFIX = "pulsar_";

	/**
	 * The prefix for the message.
	 */
	public static final String PULSAR_MESSAGE = PREFIX + "message_";

	/**
	 * Prefix for the unique message id.
	 */
	public static final String MESSAGE_ID = PULSAR_MESSAGE + "id";

	/**
	 * Prefix for the raw message data.
	 */
	public static final String RAW_DATA = PULSAR_MESSAGE + "raw_data";

	/**
	 * Prefix for message size.
	 */
	public static final String MESSAGE_SIZE = PULSAR_MESSAGE + "size";

	/**
	 * Prefix for message publish time.
	 */
	public static final String PUBLISH_TIME = PULSAR_MESSAGE + "publish_time";

	/**
	 * Prefix for event time.
	 */
	public static final String EVENT_TIME = PULSAR_MESSAGE + "event_time";

	/**
	 * Prefix for message sequence id.
	 */
	public static final String SEQUENCE_ID = PULSAR_MESSAGE + "sequence_id";

	/**
	 * prefix for the producer name.
	 */
	public static final String PRODUCER_NAME = PULSAR_MESSAGE + "producer_name";

	/**
	 * Prefix for the message key.
	 */
	public static final String KEY = PULSAR_MESSAGE + "key";

	/**
	 * Prefix for the message key as bytes.
	 */
	public static final String KEY_BYTES = PULSAR_MESSAGE + "key_bytes";

	/**
	 * Prefix for the order key.
	 */
	public static final String ORDERING_KEY = PULSAR_MESSAGE + "ordering_key";

	/**
	 * Prefix for the topic name.
	 */
	public static final String TOPIC_NAME = PULSAR_MESSAGE + "topic_name";

	/**
	 * Prefix for redelivery count.
	 */
	public static final String REDELIVERY_COUNT = PULSAR_MESSAGE + "redelivery_count";

	/**
	 * Prefix for schema version.
	 */
	public static final String SCHEMA_VERSION = PULSAR_MESSAGE + "schema_version";

	/**
	 * Prefix for the cluster replicated from.
	 */
	public static final String REPLICATED_FROM = PULSAR_MESSAGE + "replicated_from";

	/**
	 * Prefix for broker publish time.
	 */
	public static final String BROKER_PUBLISH_TIME = PULSAR_MESSAGE + "broker_publish_time";

	/**
	 * Prefix for index.
	 */
	public static final String INDEX = PULSAR_MESSAGE + "index";

}
