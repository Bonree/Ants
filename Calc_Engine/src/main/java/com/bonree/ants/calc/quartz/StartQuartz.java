package com.bonree.ants.calc.quartz;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import java.util.Properties;

import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月24日 下午8:37:02
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 定时调度任务
 * ****************************************************************************
 */
public class StartQuartz {

    private static final Logger log = LoggerFactory.getLogger(StartQuartz.class);

    /**
     * 检查redis连接的频率,单位:秒
     */
    private static final int CHECK_REDIS_INTERVAL_SEC = 3;

    /**
     * 检查kafka连接的频率,单位:秒
     */
    private static final int CHECK_KAFKA_INTERVAL_SEC = 5;

    /**
     * 概述：开启定时加载数据程序
     */
    public static void run() {
        try {
            StdSchedulerFactory ssf = new StdSchedulerFactory();
            Properties props = new Properties();
            props.put(StdSchedulerFactory.PROP_THREAD_POOL_CLASS, "org.quartz.simpl.SimpleThreadPool");
            props.put("org.quartz.threadPool.threadCount", "5");
            ssf.initialize(props);
            // 1.定时作业-检查redis集群状态
            JobDetail checkRedisJob = newJob(CheckRedisJob.class)
                    .withIdentity("checkRedisJob", Scheduler.DEFAULT_GROUP)
                    .build();

            // 定时觖发器-检查redis集群状态
            SimpleTrigger checkRedisTrigger = newTrigger()
                    .withIdentity("checkRedisJob", Scheduler.DEFAULT_GROUP)
                    .withSchedule(SimpleScheduleBuilder
                            .simpleSchedule()
                            .withIntervalInSeconds(CHECK_REDIS_INTERVAL_SEC)
                            .repeatForever())
                    .build();

            // 2.定时作业-检查kafka连接状态
            JobDetail checkKafkaJob = newJob(CheckKafkaJob.class)
                    .withIdentity("checkKafkaJob", Scheduler.DEFAULT_GROUP)
                    .build();

            // 定时觖发器-检查kafka连接状态
            SimpleTrigger checkKafkaTrigger = newTrigger()
                    .withIdentity("checkKafkaJob", Scheduler.DEFAULT_GROUP)
                    .withSchedule(SimpleScheduleBuilder
                            .simpleSchedule()
                            .withIntervalInSeconds(CHECK_KAFKA_INTERVAL_SEC)
                            .repeatForever())
                    .build();

            // 初始化调度器
            Scheduler sched = ssf.getScheduler();
            sched.scheduleJob(checkRedisJob, checkRedisTrigger);
            sched.scheduleJob(checkKafkaJob, checkKafkaTrigger);
            sched.start();
            log.info("Startup the check task scheduler !!!");
        } catch (Exception ex) {
            log.error("Startup scheduler error!", ex);
        }
    }
}
