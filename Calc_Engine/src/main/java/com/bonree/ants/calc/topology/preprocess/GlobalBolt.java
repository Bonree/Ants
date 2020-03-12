package com.bonree.ants.calc.topology.preprocess;

import bonree.proxy.JedisProxy;
import com.alibaba.fastjson.JSON;
import com.bonree.ants.commons.Commons;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.granule.GranuleCommons;
import com.bonree.ants.commons.granule.GranuleFinal;
import com.bonree.ants.commons.granule.model.GranuleMap;
import com.bonree.ants.commons.redis.JedisClient;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class GlobalBolt extends BaseAggregator<Map<String, String>> {
    private static final Logger log = LoggerFactory.getLogger(GlobalBolt.class);

    private static final long serialVersionUID = 1L;

    private int firstGranule;// 第一个粒度

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map conf, TridentOperationContext context) {
        // 初始配置参数
        Commons.initAntsSchema(conf);
        GranuleMap graMap = JSON.parseObject(GlobalContext.getConfig().getGranuleMap(), GranuleMap.class);
        firstGranule = graMap.firstValue().getGranule();
    }

    @Override
    public Map<String, String> init(Object batchId, TridentCollector collector) {
        return new HashMap<>();
    }

    @Override
    public void aggregate(Map<String, String> map, TridentTuple tuple, TridentCollector collector) {
        if (!map.containsKey(GranuleFinal.LAST_TIME_FLAG)) {
            String curTime = tuple.getStringByField("gtime");
            String bizType = tuple.getStringByField("bizType");
            map.put(GranuleFinal.LAST_TIME_FLAG, curTime);
            map.put(GranuleFinal.BIZ_TYPE_FLAG, bizType);
        }
    }

    @Override
    public void complete(Map<String, String> map, TridentCollector collector) {
        JedisProxy redis = null;
        try {
            redis = JedisClient.init();
            String curTime = map.get(GranuleFinal.LAST_TIME_FLAG);
            String bizType = map.get(GranuleFinal.BIZ_TYPE_FLAG);
            // 对计算过的数据进行打标识,方便粒度汇总使用
            GranuleCommons.granuleController(redis, bizType, firstGranule, curTime);
        } catch (Exception ex) {
            log.error("GlobalBolt error! ", ex);
        } finally {
            JedisClient.close(redis);
        }
    }
}
