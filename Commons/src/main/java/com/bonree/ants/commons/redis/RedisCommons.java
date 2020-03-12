package com.bonree.ants.commons.redis;

import bonree.proxy.JedisPipelineProxy;
import com.bonree.ants.commons.granule.GranuleCommons;
import com.bonree.ants.commons.granule.GranuleFinal;
import com.bonree.ants.commons.utils.KryoUtils;
import com.bonree.ants.plugin.etl.model.Records;
import com.bonree.ants.plugin.storage.model.DataResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018/09/30 16:07
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: Redis业务相关公共方法
 * *****************************************************************************
 */
public class RedisCommons {

    private static final Logger log = LoggerFactory.getLogger(RedisCommons.class);

    /**
     * 概述：保存数据到redis
     *
     * @param cacheMap   中间结果
     * @param bizName    数据名称
     * @param bizType    数据类型
     * @param granule    粒度
     * @param time       数据时间
     * @param batchCount 每包数据大小
     * @param bucketNum  分桶数量
     * @param dataExpire 数据过期时间
     * @param pipeline   redis连接对象
     */
    public static void saveDataToRedisByBucket(Records<Long, Object> cacheMap, String bizName, String bizType, int granule, String time, int batchCount, int bucketNum, int dataExpire, JedisPipelineProxy pipeline) {
        if (cacheMap.get().isEmpty()) {
            return;
        }
        int total = 0; // 总条数
        DataResult cachResult;
        String dataKey = null;
        long startTime = System.currentTimeMillis();
        Map<Long, DataResult> cachData = new HashMap<>();   // 存最终结果<KEY:分桶编号, 中间结果集合>
        try {
            for (Map.Entry<Long, Object> entry : cacheMap.get().entrySet()) {
                total++; // 累加总条数
                long bucket = Math.abs(entry.getKey() % bucketNum); // 分桶编号
                if (!cachData.containsKey(bucket) || cachData.get(bucket) == null) {
                    cachResult = new DataResult();
                    cachResult.setBizType(bizType);
                    cachResult.setBizName(bizName);
                    cachResult.setGranule(granule);
                    cachResult.setTime(time);
                    cachData.put(bucket, cachResult);
                }
                cachResult = cachData.get(bucket);
                cachResult.add((Records<String, Object>) entry.getValue());
                cachResult.setCount(cachResult.getCount() + 1); // 累加当前分桶的数据条数
                if (cachResult.getCount() % batchCount == 0) {
                    dataKey = String.format(GranuleFinal.DATA_KEY, bizType, granule, time, bizName, bucket);
                    saveData(dataKey, cachResult, pipeline, dataExpire); // 批量写入redis
                    cachData.put(bucket, null); // 写入当前批次后重置中间结果
                }
            }
            // 未满足批次的处理
            if (!cachData.isEmpty()) {
                for (Map.Entry<Long, DataResult> entry : cachData.entrySet()) {
                    if (entry.getValue() == null) {
                        continue;
                    }
                    dataKey = String.format(GranuleFinal.DATA_KEY, bizType, granule, time, bizName, entry.getKey());
                    saveData(dataKey, entry.getValue(), pipeline, dataExpire);
                }
            }
            log.info("Save data to redis! biz:{}, total:{}, cost: {}", dataKey, total, System.currentTimeMillis() - startTime);
        } catch (Exception ex) {
            log.error("Save data to redis error! biz:{}, total:{}, cost: {}", dataKey, total, System.currentTimeMillis() - startTime, ex);
        }
    }

    /**
     * 概述：保存中间结果数据到REDIS(外层调用此方法时,切记关闭redis连接)
     *
     * @param dataKey    存储数据时的key
     * @param result     数据结果
     * @param pipeline   redis连接对象
     * @param dataExpire 数据过期时间
     */
    private static void saveData(String dataKey, DataResult result, JedisPipelineProxy pipeline, int dataExpire) {
        try {
            byte[] bytes = KryoUtils.writeToByteArray(result);
            byte[] contents = Snappy.compress(bytes);
            byte[] dataKeys = dataKey.getBytes(StandardCharsets.UTF_8);
            pipeline.rpush(dataKeys, contents); // 保存业务数据到Redis
            pipeline.expire(dataKeys, dataExpire);
        } catch (Exception ex) {
            log.error("Save aggdata to redis error! dataKey: {}", dataKey, ex);
        }
    }
}
