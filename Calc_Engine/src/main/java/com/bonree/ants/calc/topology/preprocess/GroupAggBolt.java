package com.bonree.ants.calc.topology.preprocess;

import bonree.proxy.JedisPipelineProxy;
import bonree.proxy.JedisProxy;
import com.alibaba.fastjson.JSON;
import com.bonree.ants.calc.topology.commons.CalcCommons;
import com.bonree.ants.calc.utils.FinalUtils;
import com.bonree.ants.commons.Commons;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.enums.AggType;
import com.bonree.ants.commons.enums.BizType;
import com.bonree.ants.commons.enums.Delim;
import com.bonree.ants.commons.granule.GranuleCommons;
import com.bonree.ants.commons.granule.GranuleFinal;
import com.bonree.ants.commons.granule.model.GranuleMap;
import com.bonree.ants.commons.redis.JedisClient;
import com.bonree.ants.commons.redis.RedisCommons;
import com.bonree.ants.commons.schema.SchemaGLobals;
import com.bonree.ants.commons.schema.model.SchemaData;
import com.bonree.ants.commons.schema.model.SchemaFields;
import com.bonree.ants.commons.utils.DateFormatUtils;
import com.bonree.ants.commons.utils.KryoUtils;
import com.bonree.ants.plugin.etl.model.Records;
import com.bonree.ants.plugin.storage.model.DataResult;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class GroupAggBolt extends BaseAggregator<Map<String, Records<Long, Object>>> {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(GroupAggBolt.class);
    private long startTime;
    private JedisProxy redis;
    private GranuleMap graMap;  // 当前拓扑计算的粒度集合


    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map conf, TridentOperationContext context) {
        // 初始配置参数
        Commons.initAntsSchema(conf);
        graMap = JSON.parseObject(GlobalContext.getConfig().getGranuleMap(), GranuleMap.class);

        // 初始化redis对象
        redis = JedisClient.init();
    }

    @Override
    public Map<String, Records<Long, Object>> init(Object batchId, TridentCollector collector) {
        this.startTime = System.currentTimeMillis();
        // KEY:业务数据类型, VALUE: < KEY:分组KEY, VALUE:业务数据 >
        Map<String, Records<Long, Object>> map = new HashMap<>();
        map.put(GranuleFinal.LAST_TIME_FLAG, new Records<Long, Object>());
        return map;
    }

    @Override
    public void aggregate(Map<String, Records<Long, Object>> cacheMap, TridentTuple tuple, TridentCollector collector) {
        try {
            byte[] bizBytes = tuple.getBinaryByField("dataResult");        // 获取分区合并的数据
            DataResult data = KryoUtils.readFromByteArray(Snappy.uncompress(bizBytes));
            if (!cacheMap.get(GranuleFinal.LAST_TIME_FLAG).containsKey(GranuleFinal.FLAG)) {
                cacheMap.get(GranuleFinal.LAST_TIME_FLAG).put(GranuleFinal.FLAG, data.getTime());
            }
            GranuleCommons.aggregateData(data, cacheMap, true);
        } catch (Exception ex) {
            log.error("GroupAggBolt error! ", ex);
        }
    }

    @Override
    public void complete(Map<String, Records<Long, Object>> cacheMap, TridentCollector collector) {
        if (cacheMap.isEmpty()) {
            return;
        }
        JedisPipelineProxy pipeline = null;
        String curTime;
        String[] bizArr;
        SchemaData schema;
        try {
            if (redis == null) {
                redis = JedisClient.init();
            }
            pipeline = redis.pipeline();
            Records<Long, Object> records;
            String timeStr = cacheMap.get(GranuleFinal.LAST_TIME_FLAG).get(GranuleFinal.FLAG).toString();
            long time = Long.parseLong(timeStr);
            curTime = DateFormatUtils.format(new Date(time), GlobalContext.SEC_PATTERN);
            int curGra = GlobalContext.getConfig().getSourceGranule();
            int redisBatchCount = GlobalContext.getConfig().getRedisBatchCount();
            int redisBucketCount = graMap.firstValue().getBucket();
            int nextGranule = graMap.firstValue().getGranule();
            for (Entry<String, Records<Long, Object>> biz : cacheMap.entrySet()) { // 过滤掉内部标识后,此循环只会执行一次
                String bizKey = biz.getKey();
                if (GranuleFinal.LAST_TIME_FLAG.equals(bizKey)) {
                    continue; // 过滤掉内部标识
                }
                records = biz.getValue();
                bizArr = bizKey.split(Delim.ASCII_159.value());
                String bizType = bizArr[0];
                String bizName = bizArr[1];
                schema = SchemaGLobals.getSchemaByName(time, bizName);
                if (schema == null) {
                    log.info("CalcData not found Schema info!!! bizName: {}, time: {}", bizName, time);
                    continue;
                }
                // 1.活跃数,写入hyperlog数据到redis
                if (BizType.ACTIVE.type().equals(bizType)) {
                    String dayTime = GranuleCommons.granuleCalcStr(GranuleFinal.DAY_GRANULE, time);
                    String dayActiveKey = String.format(GranuleFinal.ACTIVE_AGG_KEY, bizType, GranuleFinal.DAY_GRANULE);
                    if (!redis.sismember(dayActiveKey, dayTime)) {
                        redis.sadd(dayActiveKey, dayTime);
                    }
                    CalcCommons.saveDataToHyperLog(records, bizName, bizType, GranuleFinal.DAY_GRANULE, dayTime, pipeline, schema);// 日活
                    String monthTime = dayTime.substring(0, 6) + "01000000";
                    int monthGranule = GranuleFinal.DAY_GRANULE * 30;
                    String monthActiveKey = String.format(GranuleFinal.ACTIVE_AGG_KEY, bizType, monthGranule);
                    if (!redis.sismember(monthActiveKey, monthTime)) {
                        redis.sadd(monthActiveKey, monthTime);
                    }
                    CalcCommons.saveDataToHyperLog(records, bizName, bizType, monthGranule, monthTime, pipeline, schema);// 月活
                    pipeline.sync();
                    continue;
                }

                if (GranuleCommons.checkCalced(redis, bizType, nextGranule, time)) {
                    log.info("Discard the calced data, name:{}, time:{}", bizName, curTime);
                    continue; // 丢弃掉已经计算过的粒度
                }

                // 2.计算中位数
                calcMedian(records, schema);

                // 3.写入业务汇总结果到redis
                RedisCommons.saveDataToRedisByBucket(records, bizName, bizType, curGra, curTime, redisBatchCount, redisBucketCount, GranuleFinal.DATA_EXPIRE, pipeline);
                pipeline.sync();
                collector.emit(new Values(bizType, curTime));
            }
            GlobalContext.LOG.debug("Preprocess GroupAgg complete! cost:{}", (System.currentTimeMillis() - this.startTime));
        } catch (Exception ex) {
            log.error("GroupAgg complete error! {}", ex);
        } finally {
            JedisClient.close(pipeline, redis);
        }
    }

    /**
     * 概述：计算中位数
     *
     * @param records 计算结果集合
     * @param schema  业务配置对象
     */
    private void calcMedian(Records<Long, Object> records, SchemaData schema) {
        String[] express;
        Object tmpObj = null;// 属性值
        Object[] medianArr; // 中位数集合
        Records<String, Object> result;
        for (Entry<Long, Object> entry : records.get().entrySet()) {
            result = (Records<String, Object>) entry.getValue();
            for (SchemaFields fields : schema.getFieldsList()) {
                String name = fields.getName();
                try {
                    if (SchemaGLobals.DIMENSION.equals(fields.getType())) {
                        continue;
                    }
                    express = fields.getExpr().split("\\$");
                    String fun = express[0];  // 计算类型(sum,max,min等)
                    if (!fun.startsWith(AggType.MEDIAN.type())) {
                        continue;
                    }
                    // 中位数处理
                    tmpObj = result.get(name);
                    if (tmpObj == null) {
                        continue;
                    }
                    medianArr = ((List<Object>) tmpObj).toArray(new Object[0]);
                    double percent = fun.equals(AggType.MEDIAN.type()) ? FinalUtils.PERCENT_50 : FinalUtils.PERCENT_90;
                    Object median = CalcCommons.calcPercentPoint(CalcCommons.subArray(medianArr), percent);
                    result.put(name, median);
                } catch (Exception ex) {
                    log.error("CalcMedian error! table:{}, field:{}, value:{}", schema.getBizName(), name, tmpObj, ex);
                }
            }
        }
    }
}
