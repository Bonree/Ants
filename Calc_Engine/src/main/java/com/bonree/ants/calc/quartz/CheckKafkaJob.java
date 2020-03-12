package com.bonree.ants.calc.quartz;

import com.bonree.ants.commons.kafka.KafkaCommons;
import com.bonree.ants.commons.kafka.KafkaGlobals;
import com.bonree.ants.commons.zookeeper.ZKCommons;
import com.bonree.ants.commons.zookeeper.ZKGlobals;
import org.apache.kafka.common.errors.NetworkException;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月25日 下午3:04:43
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 检测kafka集群状态
 * ****************************************************************************
 */
public class CheckKafkaJob implements Job {

    private static final Logger log = LoggerFactory.getLogger(CheckKafkaJob.class);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            // 1.同步发送测试消息到kafka
            SimpleProducerClient.instance.sendMsgAsync(KafkaCommons.topicNamespace(KafkaGlobals.CHECK_KAFKA_TOPIC), "200".getBytes(StandardCharsets.UTF_8));
            if (CheckFlag.getKafkaFlag()) {
                ZKCommons.instance.setData(ZKGlobals.KAFKA_MONITOR_PATH, ZKGlobals.FALSE);
            }
            CheckFlag.setKafkaFlag(false);
        } catch (NetworkException ex) {
            log.warn("Check kafka cluster warn: {}", ex.getMessage());
        } catch (Exception ex) {
            CheckFlag.setKafkaFlag(true);
            ZKCommons.instance.setData(ZKGlobals.KAFKA_MONITOR_PATH, ZKGlobals.TRUE);
            log.error("Check kafka cluster error!", ex);
        }
    }

}
