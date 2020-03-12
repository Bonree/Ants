package com.bonree.ants.extention.topology.alert;

import bonree.proxy.JedisProxy;
import com.bonree.ants.commons.enums.BizType;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.granule.GranuleCommons;
import com.bonree.ants.commons.granule.GranuleFinal;
import com.bonree.ants.commons.granule.topology.GranuleControllerSpout;
import com.bonree.ants.commons.redis.JedisClient;
import com.bonree.ants.commons.schema.SchemaGLobals;
import com.bonree.ants.commons.schema.model.SchemaData;
import com.bonree.ants.commons.utils.DateFormatUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018/11/19 09:40
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 报警汇总控制类
 * *****************************************************************************
 */
public class AggAlertSpout extends GranuleControllerSpout {

    private static final Logger log = LoggerFactory.getLogger(AggAlertSpout.class);

    private int firstGranule;//第一个粒度
    private int tailGranule; //最后一个粒度

    public AggAlertSpout(String name, int aggInterval) {
        super.topologyName = name;
        super.checkAggInterval = aggInterval;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void initConf(Map conf) {
        firstGranule = super.graMap.firstGra();
        tailGranule = super.graMap.tailGra();
    }

    @Override
    public String bizType() {
        return BizType.ALERT.type();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time", "cycleList", "bizType", "bizName", "bucketNum"));
    }

    @Override
    protected void emitData(JedisProxy jedis) {
        if (jedis == null) {
            log.warn("JedisProxy is null!");
            return;
        }
        try {
            int granule = firstGranule;
            String granuleTime = jedis.get(currGranuleKey); // 获取当前正在汇总的粒度时间
            if (StringUtils.isEmpty(granuleTime)) { // tmpCurrGra为空,表示当前粒度时各类型维度的数据已经计算完成,需要计算下一个粒度数据.
                String timeKey = String.format(GranuleFinal.AGG_TIME_KEY, bizType(), granule);  // 当前粒度汇总KEY
                granuleTime = jedis.lpop(timeKey);
                if (StringUtils.isEmpty(granuleTime)) {
                    return;
                }
                jedis.set(currGranuleKey, granuleTime);// 记录下正在计算的时间
            }
            long time = DateFormatUtils.parse(granuleTime, GlobalContext.SEC_PATTERN).getTime();
            List<String> cycleList = getCycleTime(GlobalContext.getConfig().getBaselineCycle(), tailGranule, time);
            log.info("Get cycle list, granule:{}, time: {}", tailGranule, cycleList);
            // 获取当前业务需要计算的业务名称集合,因报警和基线共用的schema,存储时用基线存的,所以这里用基线的类型来获取
            Map<String, SchemaData> bizMap = SchemaGLobals.getSchemaByType(time, BizType.BASELINE.type());
            Map<String, Map<Integer, Boolean>> bucketMap = super.getBizBucketMap(jedis, bizMap.keySet(), granule, granuleTime);
            for (Map.Entry<String, Map<Integer, Boolean>> biz : bucketMap.entrySet()) {
                String bizName = biz.getKey();
                for (Map.Entry<Integer, Boolean> bucket : biz.getValue().entrySet()) {
                    long emitTime = System.currentTimeMillis();
                    int bucketNumber = bucket.getKey();
                    boolean isTailBucket = bucket.getValue();
                    super.collector.emit(new Values(granuleTime, cycleList, bizType(), bizName, bucketNumber - 1), new Values(granuleTime, bizName, bucketNumber - 1, isTailBucket, emitTime));
                    log.info("Emit data to alertBolt, type: {}|{}|{}, bucketNumber: {}, cycleList: {}", bizName, granule, granuleTime, bucketNumber, cycleList);
                }
            }
        } catch (Exception ex) {
            log.error("EmitData error!", ex);
        }
    }

    @Override
    protected void ackMsg(Values ackValue) {
        if (ackValue.size() != 5) {
            return;
        }
        Object granuleTime = ackValue.get(0);
        Object bizName = ackValue.get(1);
        Object bucket = ackValue.get(2);
        boolean isLastBiz = Boolean.valueOf(ackValue.get(3).toString());
        long emitTime = Long.parseLong(ackValue.get(4).toString());
        if (isLastBiz) {
            JedisProxy redis = null;
            try {
                redis = JedisClient.init();
                // 计算完成,清除当前已经计算过的时间
                redis.del(currGranuleKey);
            } catch (Exception ex) {
                log.error("UploadRedisStatus error!", ex);
            } finally {
                JedisClient.close(redis);
            }
        }
        long end = System.currentTimeMillis();
        log.info("Ack success, msg: {}|{}|{}|{}|{}, costing: {}", bizType(), granuleTime, bizName, bucket, isLastBiz, (end - emitTime));
    }

    /**
     * 概述：获取基线数据对比周期的时间集合
     *
     * @param cycle   对比的周期,单位:天
     * @param granule 基线数据的粒度
     * @param time    当时基线数据的时间
     */
    private List<String> getCycleTime(int cycle, int granule, long time) {
        Date granuleDate = GranuleCommons.granuleCalc(granule, time);// 根据粒度转移时间
        List<String> cycleList = new ArrayList<>();
        for (int i = 1; i <= cycle; i++) {// 根据周期计算出对比周期的时间集合
            Date diffDate = DateFormatUtils.dayDiff(granuleDate, -i);
            cycleList.add(DateFormatUtils.format(diffDate, GlobalContext.SEC_PATTERN));
        }
        return cycleList;
    }
}
