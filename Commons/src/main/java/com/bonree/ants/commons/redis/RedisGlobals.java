package com.bonree.ants.commons.redis;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @Date: 2018年4月17日 下午8:55:36
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: redis相关配置信息
 *****************************************************************************
 */
public class RedisGlobals {

    /**
     * redis版本,默认3.0
     */
    static String REDIS_VERSION = "3.0";

    /**
     * storm操作redis的前缀
     */
    static String PREFIX_STORM = "2_9_";

    /**
     * redis上存放ETL插件的key
     */
    public static final String ETL_PLUGIN_KEY = "ants_etl_plugin_key";

    /**
     * redis上存放入库插件的key
     */
    public static final String STORAGE_PLUGIN_KEY = "ants_storage_plugin_key";

    /**
     * redis上存放自定义算子插件的key
     */
    public static final String OPERATOR_PLUGIN_KEY = "ants_operator_plugin_key";

    /**
     * 检测db环境job的同步锁key
     */
    public static final String CHECK_STORAGE_JOB_KEY = "storage_check_job_key";

    /**
     * 检测计算环境job的同步锁key
     */
    public static final String CHECK_CALC_JOB_KEY = "calc_check_job_key";

    /**
     * 各拓扑计算位置数据的过期时间,单位:秒
     */
    public final static int CALC_POSITION_EXPIRE = 30 * 60;

    /**
     * 用于表示当前粒度时间已经汇总过.
     */
    public static final String CALCED_FLAG = "-1";



}
