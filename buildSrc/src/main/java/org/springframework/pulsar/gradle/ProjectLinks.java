package org.springframework.pulsar.gradle;

public enum ProjectLinks {

	HOMEPAGE("https://github.com/spring-projects-experimental/spring-pulsar"),
	ISSUES("https://github.com/spring-projects-experimental/spring-pulsar/issues"),
	CI("https://github.com/spring-projects-experimental/spring-pulsar/actions"),
	SCM_URL("https://github.com/spring-projects-experimental/spring-pulsar"),
	SCM_CONNECTION("https://github.com/spring-projects-experimental/spring-pulsar.git"),
	SCM_DEV_CONNECTION("git@github.com:spring-projects-experimental/spring-pulsar.git");

	private final String link;

	ProjectLinks(String link) {
		this.link = link;
	}

	public String link() {
		return this.link;
	}
}
