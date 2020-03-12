package com.bonree.ants.calc.quartz;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bonree.proxy.JedisProxy;

import com.bonree.ants.commons.redis.JedisClient;
import com.bonree.ants.commons.zookeeper.ZKCommons;
import com.bonree.ants.commons.zookeeper.ZKGlobals;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月24日 下午8:39:06
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 检查redis集群状态
 * ****************************************************************************
 */
public class CheckRedisJob implements Job {

    private static final Logger log = LoggerFactory.getLogger(CheckRedisJob.class);

    private static final String SET_KEY = "SET_TEST_KEY";
    private static final String HSET_KEY = "HSET_TEST_KEY";
    private static final String LPUSH_KEY = "LPUSH_TEST_KEY";
    private static final String TEST_VALUE = "TEST_VALUE_SUCCESS";

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        JedisProxy redis = null;
        try {
            redis = JedisClient.init();
            redis.set(SET_KEY, TEST_VALUE);
            redis.get(SET_KEY);
            redis.hset(HSET_KEY, HSET_KEY, TEST_VALUE);
            redis.hget(HSET_KEY, HSET_KEY);
            redis.lpush(LPUSH_KEY, TEST_VALUE);
            redis.lpop(LPUSH_KEY);
            redis.del(SET_KEY);
            if (CheckFlag.getRedisFlag()) {
                ZKCommons.instance.setData(ZKGlobals.REDIS_MONITOR_PATH, ZKGlobals.FALSE);
            }
            CheckFlag.setSetRedisFlag(false);
        } catch (Exception ex) {
            CheckFlag.setSetRedisFlag(true);
            ZKCommons.instance.setData(ZKGlobals.REDIS_MONITOR_PATH, ZKGlobals.TRUE);
            log.error("Check redis cluster error ! {}", ex.getMessage());
        } finally {
            JedisClient.close(redis);
        }
    }
}
