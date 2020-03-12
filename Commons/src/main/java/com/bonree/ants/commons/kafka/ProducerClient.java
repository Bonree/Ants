package com.bonree.ants.commons.kafka;

import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.utils.FileUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司 Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights
 * Reserved.
 *
 * @Date: Apr 29, 2015 4:54:27 PM
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: KAKFKA生产者客户端
 * ****************************************************************************
 */
public enum ProducerClient {

    instance;

    private static final Logger log = LoggerFactory.getLogger(ProducerClient.class);

    private KafkaProducer<Integer, byte[]> producer;

    /**
     * 概述： 初始生产者
     */
    ProducerClient() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, GlobalContext.getConfig().getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 6);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 90000);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, GlobalContext.getConfig().getMsgMaxByte());
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 104857600);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 5242080);
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        producer = new KafkaProducer<>(props);
    }

    /**
     * 概述：发送消息到KAFKA服务器(带回调)
     *
     * @param topic    消息主题
     * @param msg      消息内容
     * @param filePath 发送出错时保存数据的路径
     */
    public void callbackMsg(String topic, final byte[] msg, final String filePath) {
        producer.send(new ProducerRecord<Integer, byte[]>(KafkaCommons.topicNamespace(topic), msg), new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception ex) {
                if (ex != null) {
                    if (null != filePath && !"".equals(filePath)) {
                        FileUtils.saveErrorFile(msg, GlobalContext.getConfig().getRootPath(), filePath, UUID.randomUUID().toString());
                    }
                    log.error("Send msg to kafka failed! errfile path:{}, ", filePath, ex);
                }
            }
        });// 发送
    }

    /**
     * 概述：发送消息到KAFKA服务器(同步)
     *
     * @param topic 消息主题
     * @param msg   消息内容
     */
    public RecordMetadata sendMsgAsync(String topic, byte[] msg) throws Exception {
        return producer.send(new ProducerRecord<Integer, byte[]>(KafkaCommons.topicNamespace(topic), msg)).get();// 发送
    }

    /**
     * 概述：发送消息到KAFKA服务器(异步,无回调)
     *
     * @param topic 消息主题
     * @param msg   消息内容
     */
    public void sendMsg(String topic, byte[] msg) {
        try {
            producer.send(new ProducerRecord<Integer, byte[]>(KafkaCommons.topicNamespace(topic), msg));// 发送
        } catch (Exception e) {
            log.error("send to kafka error! ", e);
        }
    }


}
