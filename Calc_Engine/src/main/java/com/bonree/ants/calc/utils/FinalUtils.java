package com.bonree.ants.calc.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月10日 下午5:24:45
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 全局常量类
 * ****************************************************************************
 */
public class FinalUtils {

    /**
     * STORM拓扑中消息超时时间(单位:秒)
     */
    public static final int MSG_TIMEOUT = 180;

    /**
     * ZK客户端连接超时
     */
    public static final int CLIENT_TIMEOUT = 60 * 1000;

    /**
     * ZK会话超时
     */
    public static final int SESSION_TIMEOUT = 180 * 1000;

    /**
     * 天粒度的汇总时间
     */
    public final static int DAY_CALC_TIME = 315;

    /**
     * 计算中位数时在过滤5%数据的集合最小数
     */
    public static final int FILTER_SIZE = 20;

    /**
     * 计算中位数时分别过滤掉上下5%的数据
     */
    public static final double PERCENT_5 = 0.05;

    /**
     * 50分位中位数
     */
    public static final double PERCENT_50 = 0.5;

    /**
     * 90分位中位数
     */
    public static final double PERCENT_90 = 0.9;

    /**
     * 计算活跃数线程
     */
    public static final ExecutorService EXECUTORS = Executors.newFixedThreadPool(2);

}
