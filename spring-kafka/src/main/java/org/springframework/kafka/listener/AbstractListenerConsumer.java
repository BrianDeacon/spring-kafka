package org.springframework.kafka.listener;

import static org.springframework.kafka.listener.AbstractMessageListenerContainer.UNUSED;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.LogIfLevelEnabled;
import org.springframework.kafka.support.TopicPartitionOffset.SeekPosition;
import org.springframework.kafka.transaction.KafkaAwareTransactionManager;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.SchedulingAwareRunnable;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

abstract class AbstractListenerConsumer<K,V> implements SchedulingAwareRunnable, ConsumerSeekCallback {

  protected static final int SIXTY = 60;
  protected final boolean autoCommit;
  protected final String consumerGroupId;
  protected final RecordInterceptor<K, V> recordInterceptor;
  protected final long maxPollInterval;
  private final LogAccessor logger = new LogAccessor(LogFactory.getLog(AbstractListenerConsumer.class)); // NOSONAR hide

  protected final ContainerProperties containerProperties;
  protected final GenericMessageListener<?> genericListener;
  protected final ListenerType listenerType;
  protected final Consumer<K, V> consumer;
  protected final Map<String, Map<Integer, Long>> offsets = new HashMap<>();
  protected final BlockingQueue<ConsumerRecord<K, V>> acks = new LinkedBlockingQueue<>();
  protected final LogIfLevelEnabled commitLogger;
  protected final Duration syncCommitTimeout;
  protected Map<TopicPartition, OffsetMetadata> definedPartitions;
  protected final KafkaMessageListenerContainer<K,V> thisContainer;
  protected OffsetCommitCallback commitCallback;
  protected volatile Thread consumerThread;
  protected long nackSleep = -1;

  protected final KafkaAwareTransactionManager kafkaTxManager;

  protected int nackIndex;

  AbstractListenerConsumer(KafkaMessageListenerContainer<K, V> thisContainer,
      GenericMessageListener<?> listener,
      ListenerType listenerType) {
    this.containerProperties = thisContainer.getContainerProperties();
    this.thisContainer = thisContainer;
    this.genericListener = listener;
    this.listenerType = listenerType;
    this.consumerGroupId = thisContainer.getGroupId();

    this.commitLogger = new LogIfLevelEnabled(this.logger, this.containerProperties.getCommitLogLevel());
    kafkaTxManager = containerProperties.getTransactionManager() instanceof KafkaAwareTransactionManager
        ? ((KafkaAwareTransactionManager) containerProperties.getTransactionManager() ) : null;

    Properties consumerProperties = new Properties(this.containerProperties.getKafkaConsumerProperties());
    this.autoCommit = determineAutoCommit(consumerProperties);
    this.consumer =
        thisContainer.getConsumerFactory().createConsumer(
            this.consumerGroupId,
            this.containerProperties.getClientId(),
            thisContainer.getClientIdSuffix(),
            consumerProperties);
    this.syncCommitTimeout = determineSyncCommitTimeout();
    this.recordInterceptor = thisContainer.getRecordInterceptor();
    this.maxPollInterval = obtainMaxPollInterval(consumerProperties);
  }


  protected void nack(int index, long sleep) {
    nack(sleep);
    nackIndex = index;
  }

  protected void nack(long sleep) {
    Assert.state(Thread.currentThread().equals(consumerThread),
        "nack() can only be called on the consumer thread");
    Assert.isTrue(sleep >= 0, "sleep cannot be negative");

    nackSleep = sleep;
  }

  protected void processAck(ConsumerRecord record) {
    if (!Thread.currentThread().equals(this.consumerThread)) {
      try {
        this.acks.put(record);
        if (this.isManualImmediateAck()) {
          this.consumer.wakeup();
        }
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new KafkaException("Interrupted while storing ack", e);
      }
    }
    else {
      if (this.isManualImmediateAck()) {
        try {
          ackImmediate(record);
        }
        catch (@SuppressWarnings(UNUSED) WakeupException e) {
          // ignore - not polling
        }
      }
      else {
        addOffset(record);
      }
    }
  }

  private void ackImmediate(ConsumerRecord record) {
    Map<TopicPartition, OffsetAndMetadata> commits = Collections.singletonMap(
        new TopicPartition(record.topic(), record.partition()),
        new OffsetAndMetadata(record.offset() + 1));
    this.commitLogger.log(() -> "Committing: " + commits);
    if (this.containerProperties.isSyncCommits()) {
      this.consumer.commitSync(commits, this.syncCommitTimeout);
    }
    else {
      this.consumer.commitAsync(commits, this.commitCallback);
    }
  }

  protected void addOffset(ConsumerRecord record) {
    this.offsets.computeIfAbsent(record.topic(), v -> new ConcurrentHashMap<>())
        .compute(record.partition(), (k, v) -> v == null ? record.offset() : Math.max(v, record.offset()));
  }

  protected Collection<ConsumerRecord<K,V>> getHighestOffsetRecords(ConsumerRecords<K,V> records) {
    return records.partitions()
        .stream()
        .collect(Collectors.toMap(tp -> tp, tp -> {
          List<ConsumerRecord<K, V>> recordList = records.records(tp);
          return recordList.get(recordList.size() - 1);
        }))
        .values();
  }

  protected boolean isManualAck() {
    return this.containerProperties.getAckMode().equals(AckMode.MANUAL);
  }

  protected boolean isCountAck() {
    return this.containerProperties.getAckMode().equals(AckMode.COUNT)
      || this.containerProperties.getAckMode().equals(AckMode.COUNT_TIME);
  }

  protected boolean isTimeOnlyAck() {
    return this.containerProperties.getAckMode().equals(AckMode.TIME);
  }

  protected boolean isManualImmediateAck() {
    return this.containerProperties.getAckMode().equals(AckMode.MANUAL_IMMEDIATE);
  }

  protected boolean isAnyManualAck() {
    return this.isManualAck() || this.isManualImmediateAck();
  }

  protected boolean isRecordAck() {
    return this.containerProperties.getAckMode().equals(AckMode.RECORD);
  }

  private boolean determineAutoCommit(Properties consumerProperties) {
    boolean isAutoCommit;
    String autoCommitOverride = consumerProperties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
    if (!this.thisContainer.getConsumerFactory().getConfigurationProperties()
            .containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
        && autoCommitOverride == null) {
      consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
      isAutoCommit = false;
    }
    else if (autoCommitOverride != null) {
      isAutoCommit = Boolean.parseBoolean(autoCommitOverride);
    }
    else {
      isAutoCommit = this.thisContainer.getConsumerFactory().isAutoCommit();
    }
    Assert.state(!this.isAnyManualAck() || !isAutoCommit,
        () -> "Consumer cannot be configured for auto commit for ackMode "
            + this.containerProperties.getAckMode());
    return isAutoCommit;
  }

  private Duration determineSyncCommitTimeout() {
    if (this.containerProperties.getSyncCommitTimeout() != null) {
      return this.containerProperties.getSyncCommitTimeout();
    }
    else {
      Object timeout = this.containerProperties.getKafkaConsumerProperties()
          .get(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
      if (timeout == null) {
        timeout = thisContainer.getConsumerFactory().getConfigurationProperties()
            .get(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
      }
      if (timeout instanceof Duration) {
        return (Duration) timeout;
      }
      else if (timeout instanceof Number) {
        return Duration.ofMillis(((Number) timeout).longValue());
      }
      else if (timeout instanceof String) {
        return Duration.ofMillis(Long.parseLong((String) timeout));
      }
      else {
        if (timeout != null) {
          Object timeoutToLog = timeout;
          this.logger.warn(() -> "Unexpected type: " + timeoutToLog.getClass().getName()
            + " in property '"
            + ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG
            + "'; defaulting to 60 seconds for sync commit timeouts");
        }
        return Duration.ofSeconds(SIXTY);
      }
    }

  }

  @Nullable
  protected TransactionTemplate determineTransactionTemplate() {
    return this.kafkaTxManager != null
        ? new TransactionTemplate(this.kafkaTxManager)
        : null;
  }

  protected BatchErrorHandler determineBatchErrorHandler(GenericErrorHandler<?> errHandler) {
    return errHandler != null ? (BatchErrorHandler) errHandler
        : this.kafkaTxManager != null ? null : new BatchLoggingErrorHandler();
  }

  protected ErrorHandler determineErrorHandler(GenericErrorHandler<?> errHandler) {
    return errHandler != null ? (ErrorHandler) errHandler
        : this.kafkaTxManager != null ? null : new LoggingErrorHandler();
  }

  protected long obtainMaxPollInterval(Properties consumerProperties) {
    Object timeout = consumerProperties.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
    if (timeout == null) {
      timeout = thisContainer.getConsumerFactory().getConfigurationProperties()
          .get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);
    }

    if (timeout instanceof Duration) {
      return ((Duration) timeout).toMillis();
    }
    else if (timeout instanceof Number) {
      return ((Number) timeout).longValue();
    }
    else if (timeout instanceof String) {
      return Long.parseLong((String) timeout);
    }
    else {
      if (timeout != null) {
        Object timeoutToLog = timeout;
        this.logger.warn(() -> "Unexpected type: " + timeoutToLog.getClass().getName()
            + " in property '"
            + ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG
            + "'; defaulting to 30 seconds.");
      }
      return Duration.ofSeconds(SIXTY / 2).toMillis(); // Default 'max.poll.interval.ms' is 30 seconds
    }
  }

  protected final class OffsetMetadata {

    protected final Long offset;

    protected final boolean relativeToCurrent;

    protected final SeekPosition seekPosition;

    OffsetMetadata(Long offset, boolean relativeToCurrent, SeekPosition seekPosition) {
      this.offset = offset;
      this.relativeToCurrent = relativeToCurrent;
      this.seekPosition = seekPosition;
    }

  }


  protected final class ConsumerBatchAcknowledgment<K2, V2> implements Acknowledgment {

    private final ConsumerRecords<K, V> records;
    private final AbstractListenerConsumer<K, V> listenerConsumer;

    ConsumerBatchAcknowledgment(ConsumerRecords<K, V> records, AbstractListenerConsumer<K, V> listenerConsumer) {
      // make a copy in case the listener alters the list
      this.records = records;
      this.listenerConsumer = listenerConsumer;
    }

    @Override
    public void acknowledge() {
      for (ConsumerRecord<K, V> record : listenerConsumer.getHighestOffsetRecords(this.records)) {
        processAck(record);
      }
    }

    @Override
    public void nack(int index, long sleep) {
      Assert.isTrue(index >= 0 && index < this.records.count(), "index out of bounds");
      listenerConsumer.nack(index, sleep);
    }

    @Override
    public String toString() {
      return "Acknowledgment for " + this.records;
    }

  }



  protected final class ConsumerAcknowledgment<K, V> implements Acknowledgment {

    private final ConsumerRecord<K, V> record;
    private final AbstractListenerConsumer<K,V> listenerConsumer;

    ConsumerAcknowledgment(ConsumerRecord<K, V> record, AbstractListenerConsumer<K,V> listenerConsumer) {
      this.record = record;
      this.listenerConsumer = listenerConsumer;
    }

    @Override
    public void acknowledge() {
      listenerConsumer.processAck(this.record);
    }

    @Override
    public void nack(long sleep) {
      listenerConsumer.nack(sleep);
    }

    @Override
    public String toString() {
      return "Acknowledgment for " + this.record;
    }

  }

}
