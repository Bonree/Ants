package com.bonree.ants.extention.topology.alert;

import bonree.proxy.JedisProxy;
import com.alibaba.fastjson.JSONObject;
import com.bonree.ants.commons.enums.Delim;
import com.bonree.ants.commons.granule.GranuleFinal;
import com.bonree.ants.commons.granule.topology.GranuleCalcBolt;
import com.bonree.ants.commons.kafka.ProducerClient;
import com.bonree.ants.commons.redis.JedisClient;
import com.bonree.ants.commons.utils.KryoUtils;
import com.bonree.ants.plugin.etl.model.Records;
import com.bonree.ants.plugin.storage.model.DataResult;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018/11/19 14:41
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 报警业务数据汇总类
 * *****************************************************************************
 */
public class AggAlertBolt extends GranuleCalcBolt {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(AggAlertBolt.class);

    private int firstGranule;//第一个粒度
    private int tailGranule; //最后一个粒度

    /**
     * 业务消息主题
     */
    private final String topic;

    public AggAlertBolt(String topic) {
        this.topic = topic;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void prepareConf(Map conf, TridentOperationContext context) {
        firstGranule = super.graMap.firstGra();
        tailGranule = super.graMap.tailGra();
    }

    @Override
    public Map<String, Records<Long, Object>> init(Object batchId, TridentCollector collector) {
        // < KEY:分组KEY(业务类型+业务名称), VALUE:业务数据<KEY:数据维度, VALUE:数据对象>>
        Map<String, Records<Long, Object>> map = new HashMap<>();
        map.put(GranuleFinal.LAST_TIME_FLAG, new Records<Long, Object>());
        map.put(GranuleFinal.CURR_BUCKET_FLAG, new Records<Long, Object>());
        return map;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void aggregate(Map<String, Records<Long, Object>> cacheMap, TridentTuple tuple, TridentCollector collector) {
        try {
            String bizType = tuple.getStringByField("bizType");     // 当前汇总的业务数据类型
            String bizName = tuple.getStringByField("bizName");     // 当前汇总的业务数据名称
            String time = tuple.getStringByField("time");           // 当前基线的时间
            Object cycleObject = tuple.getValueByField("cycleList");// 对比周期的时间集合
            int bucketNumber = tuple.getIntegerByField("bucketNum");// 当前汇总的分桶号
            if (cycleObject == null || bizName == null || bizType == null || time == null) {
                log.error("Received params is null or is empty!");
                return;
            }
            cacheMap.get(GranuleFinal.LAST_TIME_FLAG).put(GranuleFinal.FLAG, time);
            cacheMap.get(GranuleFinal.CURR_BUCKET_FLAG).put(GranuleFinal.FLAG, bucketNumber);
            List<String> cycleTimeList = (List<String>) cycleObject;
            super.aggDataByGranule(cacheMap, tailGranule, bizType, bizName, bucketNumber, cycleTimeList);
        } catch (Exception ex) {
            log.error("AggAlertBolt aggregate error !", ex);
        }
    }

    @Override
    public void complete(Map<String, Records<Long, Object>> cacheMap, TridentCollector collector) {
        JedisProxy redis = null;
        try {
            String time = (String) cacheMap.get(GranuleFinal.LAST_TIME_FLAG).get(GranuleFinal.FLAG);
            int bucketNumber = (int) cacheMap.get(GranuleFinal.CURR_BUCKET_FLAG).get(GranuleFinal.FLAG);
            redis = JedisClient.init();
            for (Map.Entry<String, Records<Long, Object>> entry : cacheMap.entrySet()) {
                if (super.skipInnerFlag(entry.getKey())) {
                    continue; // 过滤掉内部标识
                }
                String[] bizKey = entry.getKey().split(Delim.ASCII_159.value());
                if (bizKey.length != 2) {
                    log.warn("Found Illegal data: {}", entry.getKey());
                    continue; // 不合法数据
                }
                String bizType = bizKey[0];
                String bizName = bizKey[1];
                // 1. 获取一分钟的结果
                long start = System.currentTimeMillis();
                List<Records<String, Object>> recordsList = getRecordsByRedis(redis, bizType, bizName, time, firstGranule, bucketNumber);
                if (recordsList == null || recordsList.isEmpty()) {
                    continue;
                }
                long recordTime = System.currentTimeMillis();
                // 2.周期计算结果
                List<Records<String, Object>> cycleList = new ArrayList<>();
                Map<Long, ?> recordMap = entry.getValue().get();
                for (Map.Entry<Long, ?> record : recordMap.entrySet()) {
                    cycleList.add((Records<String, Object>) record.getValue());
                }
                String alarm = getCycleJsonStr(bizName, time, recordsList, cycleList);
                long jsonTime = System.currentTimeMillis();
                log.info("===json==={}", alarm);
                byte[] resultByte = alarm.getBytes(StandardCharsets.UTF_8);
                ProducerClient.instance.sendMsg(topic, resultByte);  // 发送结果数据到kafka
                log.info("Calc alert complete! recordCount: {}, cycleCount:{}, length: {}, recordCost: {}, jsonCost: {}", recordsList.size(), cycleList.size(), resultByte.length, (recordTime - start), (jsonTime - recordTime));
            }
        } catch (Exception ex) {
            log.error("completeAlerBolt error!", ex);
        } finally {
            JedisClient.close(redis);
        }

        // 5分钟的结果


    }

    /**
     * 概述：获取redis中结果数据
     *
     * @param redis        redis对象
     * @param bizType      业务类型
     * @param bizName      业务名称
     * @param granuleTime  粒度时间
     * @param granule      粒度
     * @param bucketNumber 分桶编号
     */
    private List<Records<String, Object>> getRecordsByRedis(JedisProxy redis, String bizType, String bizName, String granuleTime, int granule, int bucketNumber) {
        String dataKey = String.format(GranuleFinal.DATA_KEY, bizType, granule, granuleTime, bizName, bucketNumber);// normal#1#20151111160000#VIEW_1
        byte[] byteKey = dataKey.getBytes(StandardCharsets.UTF_8);
        if (!redis.exists(byteKey)) {
            return null;
        }
        long length = redis.llen(byteKey);
        if (length == 0) {
            log.warn("DataKey length is zero. dataKey: {}", dataKey);
        }
        DataResult result = null;
        DataResult tmpData;
        byte[] batchData;        // REDIS中存放的源粒度数据
        for (int i = 0; i < length; i++) {// 计算redis中每包数据
            try {
                batchData = redis.lindex(byteKey, i);
                if (batchData == null) {
                    log.info("Load data is empty form reids! key: {}, i: {}", dataKey, i);
                    continue;
                }
                tmpData = KryoUtils.readFromByteArray(Snappy.uncompress(batchData));
                if (result == null) {
                    result = tmpData;
                } else {
                    result.addAll(tmpData.get());
                }
            } catch (Exception ex) {
                log.error("Agg Data error ! {}", byteKey, ex);
            }
        }
        return result != null ? result.get() : null;
    }

    /**
     * 概述： 获取周期数据对比json
     *
     * @param bizName     业务名称
     * @param time        粒度时间
     * @param recordsList 一分钟计算结果
     * @param cycleList   周期计算结果
     */
    private String getCycleJsonStr(String bizName, String time, List<Records<String, Object>> recordsList, List<Records<String, Object>> cycleList) {
        JSONObject result = new JSONObject();
        result.put("bizName", bizName);
        result.put("time", time);
        result.put("record", recordsList);
        result.put("cycle", cycleList);
        return result.toJSONString();
    }

}
