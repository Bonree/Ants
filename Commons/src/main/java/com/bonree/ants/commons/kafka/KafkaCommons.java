package com.bonree.ants.commons.kafka;

import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.enums.Delim;
import com.bonree.ants.commons.utils.KryoUtils;
import com.bonree.ants.plugin.etl.model.Records;
import com.bonree.ants.plugin.storage.model.DataResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018/09/30 16:18
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: Kafka相关业务方法
 * *****************************************************************************
 */
public class KafkaCommons {

    private static final Logger log = LoggerFactory.getLogger(KafkaCommons.class);

    /**
     * 概述：保存数据到kafka
     *
     * @param cacheMap   计算结果
     * @param bizName    业务名称
     * @param granule    当前粒度
     * @param topic      消息主题
     * @param time       数据包时间
     * @param batchCount 消息包大小
     */
    public static void sendDataToKafka(Records<Long, Object> cacheMap, String bizName, int granule, String topic, String time, int batchCount) {
        DataResult result = null;
        byte[] batchResult;
        int total = 0;
        int sendCount = 0;
        long startTime = System.currentTimeMillis();
        try {
            for (Map.Entry<?, ?> data : cacheMap.get().entrySet()) { // 批量写文件
                total++; // 累计总条数
                if (result == null) {
                    result = new DataResult();
                    result.setBizName(bizName);
                    result.setGranule(granule);
                    result.setTime(time);
                }
                result.add((Records<String, Object>) data.getValue());
                if (total % batchCount == 0) {
                    batchResult = KryoUtils.writeToByteArray(result);
                    ProducerClient.instance.callbackMsg(topic, batchResult, KafkaGlobals.KAFKA_ERROR_PATH);  // 批量发送结果数据到kafka
                    sendCount++;
                    result = null;
                }
            }
            // 未满足批次的处理
            if (result != null && total > 0) {
                batchResult = KryoUtils.writeToByteArray(result);
                ProducerClient.instance.callbackMsg(topic, batchResult, KafkaGlobals.KAFKA_ERROR_PATH);  // 批量发送结果数据到kafka
                sendCount++;
            }
            if (total > 0) {
                log.info("Send data to kafka! biz:{}, granule:{}, gratime:{}, total:{}, sendCount: {} cost:{}", bizName, granule, time, total, sendCount, System.currentTimeMillis() - startTime);
            }
        } catch (Exception e) {
            log.error("Send data to kafka error! biz:{}, granule:{}, gratime:{}, total:{}, Exception :{} ,cost:{}", bizName, granule, time, total, e, System.currentTimeMillis() - startTime);
        }
    }

    /**
     * 概述：topic增加命名空间
     *
     * @param topics topic集合
     */
    public static List<String> topicNamespace(List<String> topics) {
        List<String> topicList = new ArrayList<>();
        for (String topic : topics) {
            topicList.add(topicNamespace(topic));
        }
        return topicList;
    }

    /**
     * 概述：topic增加命名空间
     *
     * @param topic topic名称
     */
    public static String topicNamespace(String topic) {
        return GlobalContext.getConfig().getNamespace() + Delim.UNDERLINE.value() + topic;
    }
}
