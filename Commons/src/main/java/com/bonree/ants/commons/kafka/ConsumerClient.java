package com.bonree.ants.commons.kafka;

import com.bonree.ants.commons.Tuple;
import com.bonree.ants.commons.config.AntsConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月10日 下午3:42:16
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 订阅kafka数据
 * ****************************************************************************
 */
public enum ConsumerClient {

    instance;

    private static final Logger log = LoggerFactory.getLogger(ConsumerClient.class);

    /**
     * 概述：数据集合
     */
    private ArrayBlockingQueue<List<byte[]>> dataQueue = new ArrayBlockingQueue<>(1);

    /**
     * poll数据时的超时时间,如果消息队列中没有消息，等待timeout毫秒后，调用poll()方法。
     * 如果队列中有消息，立即消费消息。
     */
    private final long TIMEOUT = 600;

    /**
     * 概述：初始消费者
     *
     * @param topics 订阅消息的主题(可以是多个)
     * @param config 配置信息对象
     * @return 消费者对象
     */
    private KafkaConsumer<byte[], byte[]> createConsumer(List<String> topics, AntsConfig config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group_" + (StringUtils.isEmpty(config.getGroupId()) ? topics.get(0) : config.getGroupId()));
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, config.getMsgMaxByte());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "300500"); // 默认值300500
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "90000");  // 默认值10000
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "5000");// 默认值3000
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");// 默认值300000
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.getMaxPollCount());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(topics);
        return consumer;
    }

    /**
     * 概述：拉取kafka消息
     */
    public List<byte[]> pullRecord() throws Exception {
        return dataQueue.poll();
    }

    /**
     * 概述：拉取kafka消息
     *
     * @param topics 订阅消息的主题(可以是多个)
     * @param config 配置信息对象
     */
    public void consumerRecord(List<String> topics, AntsConfig config) {
        ExecutorService executor = null;
        try {
            final String maxPollCount = config.getMaxPollCount();
            final KafkaConsumer<byte[], byte[]> consumer = createConsumer(topics, config);
            log.info("Start consumer [{}] data ......", topics);
            executor = Executors.newSingleThreadExecutor();
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    int maxCount = Integer.parseInt(maxPollCount);
                    List<byte[]> dataList = new ArrayList<>();
                    Map<Integer, Tuple<Long, Long>> cacheMap = new HashMap<>();
                    long startTime = System.currentTimeMillis();
                    while (true) {
                        try {
                            ConsumerRecords<byte[], byte[]> records = consumer.poll(TIMEOUT);  // 拉取消息
                            for (TopicPartition partition : records.partitions()) {
                                List<ConsumerRecord<byte[], byte[]>> partitionRecords = records.records(partition);
                                int partitionNum = partition.partition();
                                for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
                                    byte[] msgs = record.value();
                                    if (msgs == null || msgs.length == 0) {
                                        log.warn("Pull record.value() is empty!!!");
                                        continue;
                                    }
                                    if (!cacheMap.containsKey(record.partition())) {
                                        cacheMap.put(partitionNum, new Tuple(record.offset(), 0L));
                                    }
                                    dataList.add(msgs);
                                }
                                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                                cacheMap.get(partitionNum).setRight(lastOffset);
                                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                            }
                            if (!dataList.isEmpty()) {
                                long diffTime = System.currentTimeMillis() - startTime;
                                if (dataList.size() < maxCount && diffTime < TIMEOUT) {
                                    log.debug("Pull kafka data few ! count: {}, partition: {}, diffTime: {}", dataList.size(), cacheMap, diffTime);
                                    Thread.sleep(50);
                                    continue;
                                }
                                log.info("Pull kafka data success! count: {}, partition: {}", dataList.size(), cacheMap);
                                dataQueue.put(dataList);
                                dataList = new ArrayList<>();
                                cacheMap = new HashMap<>();
                                startTime = System.currentTimeMillis();
                            }
                        } catch (Exception ex) {
                            log.error("Consumer data error! ", ex);
                        }
                    }
                }
            });
        } catch (Exception ex) {
            if (executor != null) {
                executor.shutdown();
            }
            log.error("Consumer executor error! ", ex);
        }
    }

}
