package com.bonree.ants.calc.topology.granule;

import bonree.proxy.JedisProxy;
import com.bonree.ants.calc.topology.commons.CalcCommons;
import com.bonree.ants.calc.utils.FinalUtils;
import com.bonree.ants.commons.enums.BizType;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.granule.GranuleCommons;
import com.bonree.ants.commons.granule.GranuleFinal;
import com.bonree.ants.commons.granule.topology.GranuleControllerSpout;
import com.bonree.ants.commons.redis.JedisClient;
import com.bonree.ants.commons.redis.RedisGlobals;
import com.bonree.ants.commons.utils.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月12日 下午8:07:25
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 粒度汇总SPOUT
 * ****************************************************************************
 */
public class AggSpout extends GranuleControllerSpout {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(AggSpout.class);
    private long lastEmitTime;
    private Map<Integer, Long> calcPositionMap = new HashMap<>(); // <粒度, 数据计算完成的时间>
    private int firstGranule;// 第一个粒度
    private int tailGranule; // 最后一个粒度
    private int nextTopoGranule; // 下个拓扑的第一个粒度

    public AggSpout(String name, int aggInterval) {
        super.topologyName = name;
        super.checkAggInterval = aggInterval;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void initConf(Map conf) {
        // 当前拓扑计算的粒度集合
        firstGranule = super.graMap.firstGra(); // 当前拓扑中计算粒度的第一个粒度
        nextTopoGranule = super.graMap.tailValue().getNext();
        tailGranule = GlobalContext.getConfig().tailGranule(); // 所有汇总粒度的最后一个粒度

        String currTime = DateFormatUtils.format(new Date(), GlobalContext.SEC_PATTERN);// 清除标记
        JedisClient.setByKey(String.format(GranuleFinal.CALC_POSITION_KEY, topologyName), currTime, RedisGlobals.CALC_POSITION_EXPIRE);  // 重置拓扑计算的标记
    }

    @Override
    public String bizType() {
        return BizType.NORMAL.type();
    }

    /**
     * 概述：spout发射前的准备工作
     */
    @Override
    protected void emitPrepare(JedisProxy redis, String curGra) {
        GranuleCommons.setCalcPosition(redis, topologyName, true);
    }

    /**
     * 概述：spout发射验证,默认返回false
     */
    @Override
    protected boolean emitValidate(JedisProxy redis, int granule) {
        this.validatePosition(redis, firstGranule, topologyName, (RedisGlobals.CALC_POSITION_EXPIRE + firstGranule) * 1000);
        String nextTopologyName = GlobalContext.getConfig().nextTopology();
        if (!topologyName.equals(nextTopologyName) && GranuleCommons.emitController(redis, lastEmitTime, nextTopologyName, nextTopoGranule * 2)) {
            return true; // 当前汇总拓扑的下个拓扑计算过慢和停止时跳出
        }
        return GranuleCommons.emitController(redis, lastEmitTime, GlobalContext.getConfig().lastTopology(), 0);// 当前汇总拓扑的上个拓扑计算停止时跳出
    }

    /**
     * 概述：延迟计算
     */
    @Override
    protected boolean delayCalc(int granule, long granuleTime) {
        return GlobalContext.getConfig().firstGranule() == granule && System.currentTimeMillis() - granuleTime < GlobalContext.getConfig().getDelayCalcTime();
    }

    /**
     * 概述：两次粒度汇总间隔限制,默认fasle
     */
    @Override
    protected boolean intervalLimit(JedisProxy redis, int currGra, String granuleTime) {
        if (currGra == tailGranule) {
            if (isNormalDayGranule(granuleTime, tailGranule)) {// 判断是否为正常的天粒度
                return true;
            }
            if (redis.setnx(GranuleFinal.TAIL_INVERTAL_KEY, "") == 0) { // 设置粒度汇总间隔
                log.info("Granule {} calc skip, time: {}, less than invertal: {}", currGra, granuleTime, GranuleFinal.CALC_DAY_INTERVAL);
                return true;
            }
            redis.expire(GranuleFinal.TAIL_INVERTAL_KEY, GranuleFinal.CALC_DAY_INTERVAL);
        }
        return false;
    }

    /***
     * 概述：判断是否为正常的天粒度计算 
     */
    private static boolean isNormalDayGranule(String granuleTime, int granule) {
        if (granule != GranuleFinal.DAY_GRANULE) {// 判断是否为天粒度
            return false;
        }
        // 1.补数据汇总，返回false
        long currentTimems = System.currentTimeMillis();
        long temp = DateFormatUtils.addMins(currentTimems, -1440);
        String gTimeStr = GranuleCommons.granuleCalcStr(granule, temp);
        if (!gTimeStr.equals(granuleTime)) {
            return false;
        }
        // 2.未到指定时间，返回false
        int gMin = DateFormatUtils.getMinute(currentTimems);
        int gTime = DateFormatUtils.getHour(currentTimems) * 100 + gMin;
        return FinalUtils.DAY_CALC_TIME > gTime;
    }

    /**
     * 概述：自定义业务,这里用于处理日活计算的业务
     *
     * @param granule     当前粒度
     * @param granuleTime 当前计算的粒度时间
     */
    @Override
    protected void customCalcJob(int granule, long granuleTime) {
        if (tailGranule == granule) {
            int dayGranule = GranuleFinal.DAY_GRANULE;
            FinalUtils.EXECUTORS.submit(CalcCommons.calcActive(dayGranule)); // 计算日活

            int monthGranule = dayGranule * 30;
            FinalUtils.EXECUTORS.submit(CalcCommons.calcActive(monthGranule));// 计算月活
        }
    }

    /**
     * 概述：汇总数据后,更新redis中保存的粒度汇总的标识状态
     */
    @Override
    public void uploadRedisStatus(JedisProxy redis, String currGraTime, int curGra) throws Exception {
        lastEmitTime = DateFormatUtils.parse(currGraTime, GlobalContext.SEC_PATTERN).getTime();// 记录最后一次发射时间
        calcPositionMap.put(curGra, System.currentTimeMillis()); // 记录当前粒度计算完成的时间戳
        if (firstGranule == curGra) {
            String positionKey = String.format(GranuleFinal.CALC_POSITION_KEY, topologyName);
            String oldTime = redis.get(positionKey);
            if (calcPositionCompare(lastEmitTime, oldTime)) {
                redis.set(positionKey, currGraTime);
                redis.expire(positionKey, RedisGlobals.CALC_POSITION_EXPIRE);
            }
        }
    }

    /**
     * 概述：比较计算位置
     *
     * @param currGraTime 当前数据包时间
     * @param oldGraTime  redis中存储的位置时间
     * @return true 更新位置, false 不更新位置
     */
    private boolean calcPositionCompare(long currGraTime, String oldGraTime) throws Exception {
        long oldPositionTime = 0;
        if (oldGraTime != null) {
            oldPositionTime = DateFormatUtils.parse(oldGraTime, GlobalContext.SEC_PATTERN).getTime();
        }
        return currGraTime > oldPositionTime;
    }

    /**
     * 概述：检查计算位置,如果大于超时时间则重新赋值
     *
     * @param redis    redis对象
     * @param granule  当前粒度
     * @param topoName 拓扑名称
     * @param timeout  如果大于超时时间,则认定在超时时间内没有数据,需要设置标识位
     */
    private void validatePosition(JedisProxy redis, int granule, String topoName, long timeout) {
        long currTime = System.currentTimeMillis();
        long calcTime = calcPositionMap.containsKey(granule) ? calcPositionMap.get(granule) : 0;
        if (currTime - calcTime > timeout) {
            GranuleCommons.setCalcPosition(redis, topoName, false);
            calcPositionMap.put(granule, currTime);
        }
    }
}
