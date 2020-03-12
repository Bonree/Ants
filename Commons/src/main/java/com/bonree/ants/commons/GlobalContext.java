package com.bonree.ants.commons;

import com.bonree.ants.commons.config.AntsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年7月13日 下午1:53:26
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 全局公共变量
 * ****************************************************************************
 */
public class GlobalContext {

    /**
     * debug日志
     */
    public final static Logger LOG = LoggerFactory.getLogger("ants_calc");

    /**
     * 概述：全局配置信息
     */
    private static AntsConfig CONFIG = new AntsConfig<>();

    public static AntsConfig getConfig(){
        return CONFIG;
    }

    public static void setConfig(AntsConfig antsConfig){
        GlobalContext.CONFIG = antsConfig;
    }

    /**
     * 格式化日期-年月日时分秒
     */
    public final static String SEC_PATTERN = "yyyyMMddHHmmss";

    public final static String PREPROCESSING = "preprocessing_topology";// 原始数据ETL计算拓扑名称
    public final static String BIG_GRANULE = "big_granule_topology";    // 1小时,6小时,1天粒度汇总计算拓扑名称
    public final static String GRANULE = "small_granule_topology";      // 1分钟,10分钟准实时数据拓扑名称
    public final static String BASELINE = "baseline_topology";          // 基线结果入库拓扑名称

    /**
     * gc参数
     */
    public final static String GC_PARAMS = "%s -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=50 -XX:CMSMaxAbortablePrecleanTime=500 -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -Xloggc:/home/%s.log";

    /**
     * 配置队列大小
     */
    public final static int QUEUE_MAX_COUNT = 1;

    /**
     * app配置文件路径
     */
    public final static String APP_CONFIG_PATH = "/conf/app.xml";

}
