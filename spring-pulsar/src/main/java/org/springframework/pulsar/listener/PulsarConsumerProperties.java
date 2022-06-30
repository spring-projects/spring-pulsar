///*
// * Copyright 2022 the original author or authors.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *      https://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.springframework.pulsar.listener;
//
//import java.util.SortedMap;
//import java.util.regex.Pattern;
//
//import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
//import org.apache.pulsar.client.api.DeadLetterPolicy;
//import org.apache.pulsar.client.api.RedeliveryBackoff;
//import org.apache.pulsar.client.api.RegexSubscriptionMode;
//import org.apache.pulsar.client.api.SubscriptionInitialPosition;
//import org.apache.pulsar.client.api.SubscriptionType;
//
///**
// * @author Soby Chacko
// */
//public class PulsarConsumerProperties {
//
//	/**
//	 * Topic names.
//	 */
//	private String[] topics;
//
//	/**
//	 * Topic pattern.
//	 */
//	private Pattern topicsPattern;
//
//	private String subscriptionName;
//
//	private SubscriptionType subscriptionType;
//
//	private int receiveQueueSize;
//
//	private long acknowledgementsGroupTimeMicros;
//
//	private long negativeAckRedeliveryDelayMicros;
//
//	private int maxTotalReceiverQueueSizeAcrossPartitions;
//
//	private String consumerName;
//
//	private long ackTimeoutMillis;
//
//	private long tickDurationMillis;
//
//	private int priorityLevel;
//
//	private ConsumerCryptoFailureAction cryptoFailureAction;
//
//	private SortedMap<String, String> properties;
//
//	private boolean readCompacted;
//
//	private SubscriptionInitialPosition subscriptionInitialPosition;
//
//	private int patternAutoDiscoveryPeriod;
//
//	private RegexSubscriptionMode regexSubscriptionMode;
//
//	private DeadLetterPolicy deadLetterPolicy;
//
//	private boolean autoUpdatePartitions;
//
//	private boolean replicateSubscriptionState;
//
//	private RedeliveryBackoff negativeAckRedeliveryBackoff;
//	private RedeliveryBackoff ackTimeoutRedeliveryBackoff;
//	private boolean autoAckOldestChunkedMessageOnQueueFull;
//
//	private int maxPendingChunkedMessage;
//
//	private long expireTimeOfIncompleteChunkedMessageMillis;
//
//	public PulsarConsumerProperties(String... topics) {
//		this.topics = topics.clone();
//		this.topicsPattern = null;
//	}
//
//	public PulsarConsumerProperties(Pattern topicPattern) {
//		this.topicsPattern = topicPattern;
//		this.topics = null;
//	}
//
//	public void setTopics(String[] topics) {
//		this.topics = topics;
//	}
//
//	public void setTopicsPattern(Pattern topicsPattern) {
//		this.topicsPattern = topicsPattern;
//	}
//
//	public String getSubscriptionName() {
//		return subscriptionName;
//	}
//
//	public void setSubscriptionName(String subscriptionName) {
//		this.subscriptionName = subscriptionName;
//	}
//
//	public int getReceiveQueueSize() {
//		return receiveQueueSize;
//	}
//
//	public void setReceiveQueueSize(int receiveQueueSize) {
//		this.receiveQueueSize = receiveQueueSize;
//	}
//
//	public SubscriptionType getSubscriptionType() {
//		return subscriptionType;
//	}
//
//	public void setSubscriptionType(SubscriptionType subscriptionType) {
//		this.subscriptionType = subscriptionType;
//	}
//
//	public long getAcknowledgementsGroupTimeMicros() {
//		return acknowledgementsGroupTimeMicros;
//	}
//
//	public void setAcknowledgementsGroupTimeMicros(long acknowledgementsGroupTimeMicros) {
//		this.acknowledgementsGroupTimeMicros = acknowledgementsGroupTimeMicros;
//	}
//
//	public long getNegativeAckRedeliveryDelayMicros() {
//		return negativeAckRedeliveryDelayMicros;
//	}
//
//	public void setNegativeAckRedeliveryDelayMicros(long negativeAckRedeliveryDelayMicros) {
//		this.negativeAckRedeliveryDelayMicros = negativeAckRedeliveryDelayMicros;
//	}
//
//	public int getMaxTotalReceiverQueueSizeAcrossPartitions() {
//		return maxTotalReceiverQueueSizeAcrossPartitions;
//	}
//
//	public void setMaxTotalReceiverQueueSizeAcrossPartitions(int maxTotalReceiverQueueSizeAcrossPartitions) {
//		this.maxTotalReceiverQueueSizeAcrossPartitions = maxTotalReceiverQueueSizeAcrossPartitions;
//	}
//
//	public String getConsumerName() {
//		return consumerName;
//	}
//
//	public void setConsumerName(String consumerName) {
//		this.consumerName = consumerName;
//	}
//
//	public long getAckTimeoutMillis() {
//		return ackTimeoutMillis;
//	}
//
//	public void setAckTimeoutMillis(long ackTimeoutMillis) {
//		this.ackTimeoutMillis = ackTimeoutMillis;
//	}
//
//	public long getTickDurationMillis() {
//		return tickDurationMillis;
//	}
//
//	public void setTickDurationMillis(long tickDurationMillis) {
//		this.tickDurationMillis = tickDurationMillis;
//	}
//
//	public int getPriorityLevel() {
//		return priorityLevel;
//	}
//
//	public void setPriorityLevel(int priorityLevel) {
//		this.priorityLevel = priorityLevel;
//	}
//
//	public ConsumerCryptoFailureAction getCryptoFailureAction() {
//		return cryptoFailureAction;
//	}
//
//	public void setCryptoFailureAction(ConsumerCryptoFailureAction cryptoFailureAction) {
//		this.cryptoFailureAction = cryptoFailureAction;
//	}
//
//	public SortedMap<String, String> getProperties() {
//		return properties;
//	}
//
//	public void setProperties(SortedMap<String, String> properties) {
//		this.properties = properties;
//	}
//
//	public boolean isReadCompacted() {
//		return readCompacted;
//	}
//
//	public void setReadCompacted(boolean readCompacted) {
//		this.readCompacted = readCompacted;
//	}
//
//	public SubscriptionInitialPosition getSubscriptionInitialPosition() {
//		return subscriptionInitialPosition;
//	}
//
//	public void setSubscriptionInitialPosition(SubscriptionInitialPosition subscriptionInitialPosition) {
//		this.subscriptionInitialPosition = subscriptionInitialPosition;
//	}
//
//	public int getPatternAutoDiscoveryPeriod() {
//		return patternAutoDiscoveryPeriod;
//	}
//
//	public void setPatternAutoDiscoveryPeriod(int patternAutoDiscoveryPeriod) {
//		this.patternAutoDiscoveryPeriod = patternAutoDiscoveryPeriod;
//	}
//
//	public RegexSubscriptionMode getRegexSubscriptionMode() {
//		return regexSubscriptionMode;
//	}
//
//	public void setRegexSubscriptionMode(RegexSubscriptionMode regexSubscriptionMode) {
//		this.regexSubscriptionMode = regexSubscriptionMode;
//	}
//
//	public DeadLetterPolicy getDeadLetterPolicy() {
//		return deadLetterPolicy;
//	}
//
//	public void setDeadLetterPolicy(DeadLetterPolicy deadLetterPolicy) {
//		this.deadLetterPolicy = deadLetterPolicy;
//	}
//
//	public boolean isAutoUpdatePartitions() {
//		return autoUpdatePartitions;
//	}
//
//	public void setAutoUpdatePartitions(boolean autoUpdatePartitions) {
//		this.autoUpdatePartitions = autoUpdatePartitions;
//	}
//
//	public boolean isReplicateSubscriptionState() {
//		return replicateSubscriptionState;
//	}
//
//	public void setReplicateSubscriptionState(boolean replicateSubscriptionState) {
//		this.replicateSubscriptionState = replicateSubscriptionState;
//	}
//
//	public RedeliveryBackoff getNegativeAckRedeliveryBackoff() {
//		return negativeAckRedeliveryBackoff;
//	}
//
//	public void setNegativeAckRedeliveryBackoff(RedeliveryBackoff negativeAckRedeliveryBackoff) {
//		this.negativeAckRedeliveryBackoff = negativeAckRedeliveryBackoff;
//	}
//
//	public RedeliveryBackoff getAckTimeoutRedeliveryBackoff() {
//		return ackTimeoutRedeliveryBackoff;
//	}
//
//	public void setAckTimeoutRedeliveryBackoff(RedeliveryBackoff ackTimeoutRedeliveryBackoff) {
//		this.ackTimeoutRedeliveryBackoff = ackTimeoutRedeliveryBackoff;
//	}
//
//	public boolean isAutoAckOldestChunkedMessageOnQueueFull() {
//		return autoAckOldestChunkedMessageOnQueueFull;
//	}
//
//	public void setAutoAckOldestChunkedMessageOnQueueFull(boolean autoAckOldestChunkedMessageOnQueueFull) {
//		this.autoAckOldestChunkedMessageOnQueueFull = autoAckOldestChunkedMessageOnQueueFull;
//	}
//
//	public int getMaxPendingChunkedMessage() {
//		return maxPendingChunkedMessage;
//	}
//
//	public void setMaxPendingChunkedMessage(int maxPendingChunkedMessage) {
//		this.maxPendingChunkedMessage = maxPendingChunkedMessage;
//	}
//
//	public long getExpireTimeOfIncompleteChunkedMessageMillis() {
//		return expireTimeOfIncompleteChunkedMessageMillis;
//	}
//
//	public void setExpireTimeOfIncompleteChunkedMessageMillis(long expireTimeOfIncompleteChunkedMessageMillis) {
//		this.expireTimeOfIncompleteChunkedMessageMillis = expireTimeOfIncompleteChunkedMessageMillis;
//	}
//
//	public String[] getTopics() {
//		return topics;
//	}
//
//	public Pattern getTopicsPattern() {
//		return topicsPattern;
//	}
//
//
//}
