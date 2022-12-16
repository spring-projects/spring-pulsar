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

package org.springframework.pulsar.support.converter;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Batch records message converter.
 *
 * @param <T> message type.
 * @author Soby Chacko
 */
public class PulsarBatchMessagingMessageConverter<T> implements PulsarBatchMessageConverter<T> {

	private final PulsarRecordMessageConverter<T> recordConverter;

	public PulsarBatchMessagingMessageConverter() {
		this(null);
	}

	public PulsarBatchMessagingMessageConverter(PulsarRecordMessageConverter<T> recordConverter) {
		this.recordConverter = recordConverter;
	}

	private Object obtainPayload(Type type, org.apache.pulsar.client.api.Message<T> record,
			List<Exception> conversionFailures) {
		return this.recordConverter == null || !containerType(type) ? extractAndConvertValue(record, type)
				: convert(record, type, conversionFailures);
	}

	private boolean containerType(Type type) {
		return type instanceof ParameterizedType && ((ParameterizedType) type).getActualTypeArguments().length == 1;
	}

	protected Object extractAndConvertValue(org.apache.pulsar.client.api.Message<T> record, Type type) {
		return record.getValue();
	}

	protected Object convert(org.apache.pulsar.client.api.Message<T> record, Type type,
			List<Exception> conversionFailures) {
		try {
			Object payload = this.recordConverter
					.toMessage(record, null, ((ParameterizedType) type).getActualTypeArguments()[0]).getPayload();
			conversionFailures.add(null);
			return payload;
		}
		catch (Exception ex) {
			throw new RuntimeException("The batch converter can only report conversion failures to the listener "
					+ "if the record.value() is byte[], Bytes, or String", ex);
		}
	}

}
