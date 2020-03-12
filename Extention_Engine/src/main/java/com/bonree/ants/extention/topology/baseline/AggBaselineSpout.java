package com.bonree.ants.extention.topology.baseline;

import bonree.proxy.JedisProxy;
import com.bonree.ants.commons.enums.BizType;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.granule.GranuleCommons;
import com.bonree.ants.commons.granule.GranuleFinal;
import com.bonree.ants.commons.granule.topology.GranuleControllerSpout;
import com.bonree.ants.commons.utils.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年8月8日 下午2:45:00
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 基线数据汇总控制SPOUT
 * ****************************************************************************
 */
public class AggBaselineSpout extends GranuleControllerSpout {

    private static final long serialVersionUID = 1L;

    private static final Logger log = LoggerFactory.getLogger(AggBaselineSpout.class);

    private volatile long lastEmitTime;    // 上次汇总数据的时间
    private int firstGranule;              // 当前粒度集合的第一个粒度

    public AggBaselineSpout(String name, int aggInterval) {
        super.topologyName = name;
        super.checkAggInterval = aggInterval;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void initConf(Map conf) {
        firstGranule = super.graMap.firstGra();
    }

    @Override
    public String bizType() {
        return BizType.BASELINE.type();
    }

    /**
     * 概述：spout发射验证,默认返回false
     */
    @Override
    protected boolean emitValidate(JedisProxy redis, int granule) {
        // 预处理拓扑计算停止时跳出
        return GranuleCommons.emitController(redis, lastEmitTime, GlobalContext.PREPROCESSING, 0);
    }

    /**
     * 概述：更新redis中保存的粒度汇总的标识状态
     */
    @Override
    public void uploadRedisStatus(JedisProxy redis, String currGraTime, int granule) throws Exception {
        // 记录最后一次发射时间
        lastEmitTime = DateFormatUtils.parse(currGraTime, GlobalContext.SEC_PATTERN).getTime();

        // 保存报警数据的标识
        if (firstGranule == granule) {
            String curGraKey = String.format(GranuleFinal.AGG_TIME_KEY, BizType.ALERT.type(), granule);  // 当前粒度汇总KEY
            redis.rpush(curGraKey, currGraTime);
            log.info("Save alert time key, granule: {}, time: {}", granule, currGraTime);
        }
    }
}
