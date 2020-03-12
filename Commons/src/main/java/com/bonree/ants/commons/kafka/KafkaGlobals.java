package com.bonree.ants.commons.kafka;

import java.util.Collections;
import java.util.List;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @Date: 2018年4月17日 下午9:34:58
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: kafka相关参数信息
 *****************************************************************************
 */
public class KafkaGlobals {

    /**
     * 业务数据TOPIC
     */
    public static List<String> SOURCE_TOPIC = Collections.singletonList("apms_sdk_topic");

    /**
     * 报警结果存储topic
     */
    public static String ALERT_TOPIC = "alert_topic";

    /***
     * 验证kafka集群连接的topic
     */
    public static String CHECK_KAFKA_TOPIC = "valid_topic";

    /**
     * 接收数据库连接失败时正在处理数据的topic
     */
    public static String CONNECTION_FAIL_TOPIC = "fail_topic";

    /**
     * kafka出错时计算结果保存文件的路径
     */
    public static final String KAFKA_ERROR_PATH = "/errfile/kafka/result";

    /**
     * kafka出错并且快照数据保存开关打开时保存文件的路径
     */
    public static final String KAFKA_SNAPSHOT_ERROR_PATH = "/errfile/kafka/snapshot";

    /**
     * 错误文件存储的路径
     */
    public static final String ERROR_FILE_PATH = "/errfile/db";

    /**
     * 恢复kafka出错保存的文件时用的同步锁的key
     */
    public static final String KAFKA_ERROR_FILE_KEY = "KAFKA_ERROR_FILE_KEY";

    /**
     * 恢复kafka出错保存的文件时用的频率
     */
    public static final int KAFKA_ERROR_FILE_EXPIRE = 60 * 60;

    /**
     * 各个分区kafka消费时间列表
     */
    public static final String SDK_CONSUMER_TIME_KEY = "SDK_CONSUMER_TIME";

}
