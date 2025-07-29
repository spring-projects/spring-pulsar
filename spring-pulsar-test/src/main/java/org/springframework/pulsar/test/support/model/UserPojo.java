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

package org.springframework.pulsar.test.support.model;

import java.util.Objects;

/**
 * Test object (user) defined via standard Java beans get/set methods.
 * <p>
 * <b>WARN</b> Do not convert this to a Record as this is used for Avro tests and Avro
 * does not work well w/ records yet.
 *
 * @deprecated this class is replaced with Gradle test fixtures and is only meant to be
 * used internally.
 */
@Deprecated(since = "1.2.0", forRemoval = true)
public class UserPojo {

	private String name;

	private int age;

	UserPojo() {
	}

	public UserPojo(String name, int age) {
		this.name = name;
		this.age = age;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		UserPojo user = (UserPojo) o;
		return age == user.age && Objects.equals(name, user.name);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, age);
	}

	@Override
	public String toString() {
		return "User{" + "name='" + name + '\'' + ", age=" + age + '}';
	}

}
