/*
 * Copyright 2024 the original author or authors.
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

package org.springframework.pulsar.test.support;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.pulsar.client.api.Message;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests for {@link ConsumedMessagesCondition}.
 *
 * @author Jonas Geiregat
 */
class ConsumedMessagesConditionTest {

	@ParameterizedTest
	@CsvSource({ "true, true, true", "true, false, false", "false, true, false", "false, false, false" })
	void bothConditionsShouldBeMetForAndToBeMet(boolean result1, boolean result2, boolean expected) {
		var condition1 = new TestConsumedMessagesCondition(result1);
		var condition2 = new TestConsumedMessagesCondition(result2);

		assertThat(condition1.and(condition2).meets(List.of())).isEqualTo(expected);
	}

	static class TestConsumedMessagesCondition implements ConsumedMessagesCondition<String> {

		private final boolean result;

		TestConsumedMessagesCondition(boolean result) {
			this.result = result;
		}

		@Override
		public boolean meets(List<Message<String>> messages) {
			return result;
		}

	}

}
