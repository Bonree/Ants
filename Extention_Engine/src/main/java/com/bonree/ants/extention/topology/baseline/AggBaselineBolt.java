package com.bonree.ants.extention.topology.baseline;

import bonree.proxy.JedisPipelineProxy;
import bonree.proxy.JedisProxy;
import com.bonree.ants.commons.enums.BizType;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.granule.topology.GranuleCalcBolt;
import com.bonree.ants.commons.redis.JedisClient;
import com.bonree.ants.commons.redis.RedisCommons;
import com.bonree.ants.plugin.etl.model.Records;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年8月8日 下午3:01:00
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 基线数据汇总BOLT
 * ****************************************************************************
 */
public class AggBaselineBolt extends GranuleCalcBolt {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(AggBaselineBolt.class);

    private JedisProxy redis;   // redis集群对象

    @SuppressWarnings("rawtypes")
    @Override
    public void prepareConf(Map conf, TridentOperationContext context) {
        redis = JedisClient.init(); // 初始redis集群对象
    }

    @Override
    public void calcResult(Records<Long, Object> resultMap, String bizName, int granule, String granuleTime) {
        // 保存基线数据到redis
        JedisPipelineProxy pipeline = null;
        try {
            pipeline = redis.pipeline();
            log.info("baseline result:{}", resultMap);
            String bizType = BizType.ALERT.type(); // 这里设置为报警的类型,是因为保存的数据要被报警拓扑使用
            int redisBatchCount = GlobalContext.getConfig().getRedisBatchCount();
            int dataExpire = GlobalContext.getConfig().getBaselineExpire();
            int redisBucketCount = super.graMap.get(granule).getBucket();
            RedisCommons.saveDataToRedisByBucket(resultMap, bizName, bizType, granule, granuleTime, redisBatchCount, redisBucketCount, dataExpire, pipeline);
        } catch (Exception ex) {
            log.error("Save baseline result error ! {}", ex);
        } finally {
            JedisClient.close(pipeline, redis);
        }
    }
}
