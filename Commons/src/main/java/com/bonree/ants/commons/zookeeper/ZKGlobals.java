package com.bonree.ants.commons.zookeeper;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @Date: 2018年4月24日 下午2:12:28
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: zookeeper相关参数信息
 *****************************************************************************
 */
public class ZKGlobals {

    /**
     * SCHEMA配置信息(schema.xml)存储的路径(upload时监听使用)
     */
    public static final String SCHEMA_CONFIG_PATH = "/ants/config/schema";
    
    /**
     * SCHEMA配置信息(schema目录下所有的业务表)存储的路径
     */
    public static final String SCHEMA_CALC_CONFIG_PATH = "/ants/config/calc/schema";
    
    /**
     * SCHEMA配置信息存储的路径
     */
    public static final String SCHEMA_INSERT_CONFIG_PATH = "/ants/config/insert/schema";
    
    /**
     * zk上监听redis状态的路径
     */
    public static final String REDIS_MONITOR_PATH = "/ants/redis/status";

    /**
     * zk上监听db状态的路径
     */
    public static final String DB_MONITOR_PATH = "/ants/db/status";

    /**
     * zk上监听kafka状态的路径
     */
    public static final String KAFKA_MONITOR_PATH = "/ants/kafka/status";
    
    /**
     * zk上监听状态的值:表示需要暂停业务逻辑处理
     */
    public static final String TRUE = "true";

    /**
     * zk上监听状态的值:表示需要可以正常业务逻辑处理
     */
    public static final String FALSE = "false";

}
