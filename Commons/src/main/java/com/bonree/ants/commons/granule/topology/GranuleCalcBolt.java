package com.bonree.ants.commons.granule.topology;

import bonree.proxy.JedisPipelineProxy;
import bonree.proxy.JedisProxy;
import com.alibaba.fastjson.JSON;
import com.bonree.ants.commons.Commons;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.granule.GranuleCommons;
import com.bonree.ants.commons.granule.GranuleFinal;
import com.bonree.ants.commons.granule.model.GranuleMap;
import com.bonree.ants.commons.redis.JedisClient;
import com.bonree.ants.commons.redis.RedisCommons;
import com.bonree.ants.commons.utils.KryoUtils;
import com.bonree.ants.plugin.etl.model.Records;
import com.bonree.ants.plugin.storage.model.DataResult;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;

/**
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018/8/30 14:10
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 粒度汇总业务计算类
 * *****************************************************************************
 */
public abstract class GranuleCalcBolt extends BaseAggregator<Map<String, Records<Long, Object>>> {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(GranuleCalcBolt.class);

    private JedisProxy redis;  // redis对象
    protected GranuleMap graMap; // 当前拓扑计算的粒度集合

    /**
     * 概述：初始配置参数
     *
     * @param conf    配置参数
     * @param context 上下文信息
     */
    @SuppressWarnings("rawtypes")
    protected void prepareConf(Map conf, TridentOperationContext context) {

    }

    /**
     * 概述：计算结果处理,由子类实现完成
     *
     * @param resultMap   计算结果
     * @param bizName     数据名称
     * @param granule     数据粒度
     * @param granuleTime 数据的粒度时间
     */
    protected void calcResult(Records<Long, Object> resultMap, String bizName, int granule, String granuleTime) {

    }

    /**
     * 概述：在Init函数中根据子类需要做一些初始操作, 每个分组类型调用一次此函数
     */
    protected void init() {

    }

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map conf, TridentOperationContext context) {
        Commons.initAntsSchema(conf);
        graMap = JSON.parseObject(GlobalContext.getConfig().getGranuleMap(), GranuleMap.class);
        prepareConf(conf, context);

        // 初始化redis对象
        redis = JedisClient.init();
    }

    @Override
    public Map<String, Records<Long, Object>> init(Object batchId, TridentCollector collector) {
        init();
        // < KEY:分组KEY(业务类型+业务名称), VALUE:业务数据<KEY:数据维度CODE, VALUE:数据对象>>
        Map<String, Records<Long, Object>> map = new HashMap<>();
        map.put(GranuleFinal.CURR_GRANULE_FLAG, new Records<Long, Object>());
        map.put(GranuleFinal.LAST_TIME_FLAG, new Records<Long, Object>());
        map.put(GranuleFinal.CALCED_TIME_FLAG, new Records<Long, Object>());
        map.put(GranuleFinal.BIZ_TYPE_FLAG, new Records<Long, Object>());
        map.put(GranuleFinal.BIZ_NAME_FLAG, new Records<Long, Object>());
        return map;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void aggregate(Map<String, Records<Long, Object>> cacheMap, TridentTuple tuple, TridentCollector collector) {
        try {
            int granule = tuple.getIntegerByField("granule");           // 当前汇总的粒度
            String bizType = tuple.getStringByField("bizType");         // 当前汇总的业务数据类型
            String bizName = tuple.getStringByField("bizName");         // 当前汇总的业务数据名称
            Object tempList = tuple.getValueByField("timeList");        // 当前汇总的时间集合
            int bucketNum = tuple.getIntegerByField("bucketNum");       // 当前汇总的分桶号
            if (bizName == null || bizType == null || tempList == null) {
                log.error("Received params is null or is empty!");
                return;
            }
            List<String> upGraTimeList = (List<String>) tempList;
            if (upGraTimeList.isEmpty()) {
                log.error("Received params tempList is empty!");
                return;
            }
            cacheMap.get(GranuleFinal.CURR_GRANULE_FLAG).put(GranuleFinal.FLAG, granule);
            cacheMap.get(GranuleFinal.BIZ_TYPE_FLAG).put(GranuleFinal.FLAG, bizType);
            cacheMap.get(GranuleFinal.BIZ_NAME_FLAG).put(GranuleFinal.FLAG, bizName);
            cacheMap.get(GranuleFinal.LAST_TIME_FLAG).put(GranuleFinal.FLAG, upGraTimeList.get(upGraTimeList.size() - 1));
            // 汇总各维度数据
            aggDataByGranule(cacheMap, graMap.lastGra(granule), bizType, bizName, bucketNum, upGraTimeList);
        } catch (Exception ex) {
            log.error("AggBolt aggregate error !", ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void complete(Map<String, Records<Long, Object>> cacheMap, TridentCollector collector) {
        JedisPipelineProxy pipeline = null;
        try {
            HashSet<String> dataKeyList = (HashSet<String>) cacheMap.get(GranuleFinal.CALCED_TIME_FLAG).get(GranuleFinal.FLAG);
            int granule = (Integer) cacheMap.get(GranuleFinal.CURR_GRANULE_FLAG).get(GranuleFinal.FLAG);
            String lastTimeKey = (String) cacheMap.get(GranuleFinal.LAST_TIME_FLAG).get(GranuleFinal.FLAG);
            String bizType = (String) cacheMap.get(GranuleFinal.BIZ_TYPE_FLAG).get(GranuleFinal.FLAG);
            String bizName = (String) cacheMap.get(GranuleFinal.BIZ_NAME_FLAG).get(GranuleFinal.FLAG);
            String granuleTime = GranuleCommons.granuleCalcStr(granule, lastTimeKey);
            pipeline = redis.pipeline();
            for (Entry<String, Records<Long, Object>> entry : cacheMap.entrySet()) {
                String bizKey = entry.getKey();
                if (skipInnerFlag(bizKey)) {
                    continue; // 过滤掉内部标识
                }
                // 处理计算结果,由子类实现
                calcResult(entry.getValue(), bizName, granule, granuleTime);
                // 保存中间结果数据到redis
                int tailGranule = GlobalContext.getConfig().tailGranule();
                if (granule != tailGranule) { // 跳过不保存redis的中间结果,一般为最后一个粒度
                    int redisBatchCount = GlobalContext.getConfig().getRedisBatchCount();
                    int redisBucketCount = graMap.get(granule).getNextBucket();
                    RedisCommons.saveDataToRedisByBucket(entry.getValue(), bizName, bizType, granule, granuleTime, redisBatchCount, redisBucketCount, GranuleFinal.DATA_EXPIRE, pipeline);
                }
            }
            // 删除已经汇总过的各维度的数据粒度KEY.
            if (dataKeyList != null) {
                for (String key : dataKeyList) {
                    try {
                        pipeline.expire(key.getBytes(StandardCharsets.UTF_8), 1);
                    } catch (Exception ex) {
                        log.error("Expire datakey error! {}", key, ex);
                    }
                }
                if (!dataKeyList.isEmpty()) {
                    GlobalContext.LOG.debug("Expire datakey: {}", dataKeyList);
                }
            }
            pipeline.sync();
        } catch (Exception ex) {
            log.error("AggBolt complete error !", ex);
        } finally {
            JedisClient.close(pipeline, redis);
        }
    }

    /**
     * 概述：根据时间粒度汇总数据
     *
     * @param cacheMap      缓存计算结果
     * @param lastGra       汇总当前数据所需要的时间粒度(上个粒度)
     * @param bizType       业务数据类型
     * @param bizName       业务数据名称
     * @param bucket        当前计算的数据的桶编号
     * @param upGraTimeList 获取数据的时间key集合
     */
    protected void aggDataByGranule(Map<String, Records<Long, Object>> cacheMap, int lastGra, String bizType, String bizName, int bucket, List<String> upGraTimeList) {
        Set<String> dataKeyList = new HashSet<>();
        byte[] batchData;        // REDIS中存放的源粒度数据
        byte[] byteKey;
        DataResult data;
        try {
            for (String time : upGraTimeList) { // 遍历汇总当前粒度所需要的源数据
                String dataKey = String.format(GranuleFinal.DATA_KEY, bizType, lastGra, time, bizName, bucket);// normal#1#20151111160000#VIEW_1
                dataKeyList.add(dataKey);
                byteKey = dataKey.getBytes(StandardCharsets.UTF_8);
                if (!redis.exists(byteKey)) {
                    continue;
                }
                long length = redis.llen(byteKey);
                if (length == 0) {
                    log.warn("DataKey length is zero. dataKey: {}", dataKey);
                }
                for (int i = 0; i < length; i++) {// 计算redis中每包数据
                    try {
                        batchData = redis.lindex(byteKey, i);
                        if (batchData == null) {
                            log.info("Load data is empty form reids! key: {}, i: {}", dataKey, i);
                            continue;
                        }
                        data = KryoUtils.readFromByteArray(Snappy.uncompress(batchData));
                        GranuleCommons.aggregateData(data, cacheMap, false);
                    } catch (ArrayIndexOutOfBoundsException ex) {
                        log.error("Agg Data ArrayIndexOutOfBoundsException ! lastGraKey: {}, length: {}, i: {}", dataKey, length, i, ex);
                    } catch (Exception ex) {
                        log.error("Agg Data error ! lastGraKey: {}", dataKey, ex);
                    }
                }
            }
            cacheMap.get(GranuleFinal.CALCED_TIME_FLAG).put(GranuleFinal.FLAG, dataKeyList);
        } catch (Exception ex) {
            log.error("AggDataByGranule error!", ex);
        } finally {
            JedisClient.close(redis);
        }
    }

    /**
     * 概述：需要跳过的内部标识
     *
     * @param bizName 数据名称
     * @return true 过滤, false 不过滤
     */
    protected boolean skipInnerFlag(String bizName) {
        // 过滤掉内部标识
        return GranuleFinal.CALCED_TIME_FLAG.equals(bizName) || GranuleFinal.CURR_BUCKET_FLAG.equals(bizName) || GranuleFinal.CURR_GRANULE_FLAG.equals(bizName) || GranuleFinal.BIZ_TYPE_FLAG.equals(bizName) || GranuleFinal.LAST_TIME_FLAG.equals(bizName) || GranuleFinal.BIZ_NAME_FLAG.equals(bizName);
    }
}
