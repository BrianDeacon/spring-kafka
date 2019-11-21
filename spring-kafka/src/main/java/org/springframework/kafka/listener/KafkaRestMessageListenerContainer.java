package org.springframework.kafka.listener;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaResourceHolder;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.kafka.support.LogIfLevelEnabled;
import org.springframework.kafka.support.SeekUtils;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.TopicPartitionOffset.SeekPosition;
import org.springframework.kafka.support.TransactionSupport;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer2;
import org.springframework.kafka.transaction.KafkaAwareTransactionManager;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.SchedulingAwareRunnable;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

public class KafkaRestMessageListenerContainer<K, V> extends KafkaMessageListenerContainer<K, V> {

  public KafkaRestMessageListenerContainer(
      ConsumerFactory<? super K, ? super V> consumerFactory,
      ContainerProperties containerProperties) {
    super(consumerFactory, containerProperties);
  }

  KafkaRestMessageListenerContainer(
      AbstractMessageListenerContainer<K, V> container,
      ConsumerFactory<? super K, ? super V> consumerFactory,
      ContainerProperties containerProperties) {
    super(container, consumerFactory, containerProperties);
  }

  @Override
  @Nullable
  public Collection<TopicPartition> getAssignedPartitions() {
    // Looks like this delegates to just super.listenerConsumer, which
    // is provided in the ContainerProperties
    return super.getAssignedPartitions();
  }

  @Override
  public boolean isContainerPaused() {
    // Same as getAssignedPartitions
    return super.isContainerPaused();
  }

  @Override
  public Map<String, Map<MetricName, ? extends Metric>> metrics() {
    // Same
    return super.metrics();
  }

  @Override
  protected void doStart() {
    // Calls super.checkTopics, which makes an AdminClient, which is client specific

    /* This is the meat of what doStart accomplishes.
    It's a KafkaMessageListenerContainer.ListenerConsumer, which is a private class,
    and that thing is the real implementation.
     */

    /*
    this.listenerConsumerFuture = containerProperties
				.getConsumerTaskExecutor()
				.submitListenable(this.listenerConsumer);
     */

    super.doStart();

  }

  @Override
  protected void checkTopics() {

  }

  @Override
  protected void doStop(final Runnable callback) {
    super.doStop(callback);
  }



}
