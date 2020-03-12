package com.bonree.ants.commons.plugin;

import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.plugin.model.EtlPlugin;
import com.bonree.ants.commons.plugin.model.OperatorPlugin;
import com.bonree.ants.commons.plugin.model.StoragePlugin;
import com.bonree.ants.commons.utils.DateFormatUtils;
import com.bonree.ants.commons.utils.LimitQueue;

import java.util.Date;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @Date: 2018年5月3日 上午10:01:23
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 插件相关全局属性
 *****************************************************************************
 */
public class PluginGlobals {
    /**
     * ETL插件所在的路径(分散在各个计算节点)
     */
    public final static String ETL_PATH = "/data/plugin/etl";

    /**
     * storage插件所在的路径(分散在各个计算节点)
     */
    public final static String STORAGE_PATH = "/data/plugin/storage";

    /**
     * 自定义算子插件所在的路径(分散在各个计算节点)
     */
    public final static String OPERATOR_PATH = "/data/plugin/operator";

    /**
     * etl插件队列 
     */
    public static LimitQueue<EtlPlugin> ETL_QUEUE = new LimitQueue<>(GlobalContext.QUEUE_MAX_COUNT);

    /**
     * Storage插件队列 
     */
    public static LimitQueue<StoragePlugin> STORAGE_QUEUE = new LimitQueue<>(GlobalContext.QUEUE_MAX_COUNT);

    /**
     * Operator插件队列 
     */
    public static LimitQueue<OperatorPlugin> OPERATOR_QUEUE = new LimitQueue<>(GlobalContext.QUEUE_MAX_COUNT);

    /**
     * 概述：根据时间获取队列中的对象
     * @param time 时间
     */
    public static <T> T get(long time, LimitQueue<T> queue) {
        long tmpTime = DateFormatUtils.getDay(new Date(time)).getTime();
        boolean flag = false;
        T tmpPlugin = null;
        for (T plugin : queue.getQueue()) {
            long pluginTime = getPluginTime(plugin);
            if (tmpTime == pluginTime) {
                return plugin;
            }
            if (tmpTime > pluginTime) {
                tmpPlugin = plugin;
                flag = true;
            }
            if (tmpTime < pluginTime) {
                if (flag) {
                    break;
                }
                tmpPlugin = plugin;
            }
        }
        return tmpPlugin;
    }

    /**
     * 概述：获取插件的创建时间
     * @param plugin 插件对象
     */
    public static <T> long getPluginTime(T plugin) {
        if (plugin instanceof EtlPlugin) {
            return ((EtlPlugin) plugin).getCreateTime();
        } else if (plugin instanceof StoragePlugin) {
            return ((StoragePlugin) plugin).getCreateTime();
        }
        return 0;
    }
}
