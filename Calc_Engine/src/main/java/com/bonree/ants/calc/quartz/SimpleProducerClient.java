package com.bonree.ants.calc.quartz;

import com.bonree.ants.commons.GlobalContext;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Properties;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月25日 下午7:14:50
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 此生产者主要用于验证kafka集群连接
 * ****************************************************************************
 */
public enum SimpleProducerClient {

    instance;

    private KafkaProducer<Integer, byte[]> producer;

    /**
     * 概述： 初始生产者
     */
    SimpleProducerClient() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, GlobalContext.getConfig().getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "90000");
        props.put(ProducerConfig.RETRIES_CONFIG, "20");
        producer = new KafkaProducer<>(props);
    }

    /**
     * 概述：发送消息到KAFKA服务器(同步)
     *
     * @param topic 消息主题
     * @param msg   消息内容
     */
    public void sendMsgAsync(String topic, byte[] msg) throws Exception {
        producer.send(new ProducerRecord<Integer, byte[]>(topic, msg)).get();// 发送
    }

    public void close(KafkaProducer<Integer, byte[]> producer) {
        if (producer != null) {
            producer.close();    // 关闭客户端
        }
    }
}
