package com.bonree.ants.storage.quartz;

import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @Date: 2018年4月24日 下午8:37:02
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 定时调度任务
 *****************************************************************************
 */
public class StartQuartz {

    private static final Logger log = LoggerFactory.getLogger(StartQuartz.class);

    /**
     * 检查db连接的频率,单位:秒
     */
    private static final int CHECK_DB_INTERVAL_SEC = 60;

    /**
     * 概述：开启定时加载数据程序
     */
    public static void run() {
        try {
            StdSchedulerFactory ssf = new StdSchedulerFactory();
            Properties props = new Properties();
            props.put(StdSchedulerFactory.PROP_THREAD_POOL_CLASS, "org.quartz.simpl.SimpleThreadPool");
            props.put("org.quartz.threadPool.threadCount", "2");
            ssf.initialize(props);

            // 定时作业-检查数据库连接状态
            JobDetail checkDBJob = newJob(CheckDBJob.class)
                    .withIdentity("checkDBJob", Scheduler.DEFAULT_GROUP)
                    .build();

            // 定时觖发器-检查数据库连接状态
            SimpleTrigger checkDBTrigger = newTrigger()
                    .withIdentity("checkDBJob", Scheduler.DEFAULT_GROUP)
                    .withSchedule(SimpleScheduleBuilder
                            .simpleSchedule()
                            .withIntervalInSeconds(CHECK_DB_INTERVAL_SEC)
                            .repeatForever())
                    .build();


            // 初始化调度器
            Scheduler sched = ssf.getScheduler();
            sched.scheduleJob(checkDBJob, checkDBTrigger);
            sched.start();
            log.info("Startup the check db task scheduler !!!");
        } catch (Exception ex) {
            log.error("Startup scheduler error!", ex);
        }
    }
}
