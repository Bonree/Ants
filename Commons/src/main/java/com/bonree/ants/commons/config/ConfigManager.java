package com.bonree.ants.commons.config;

import com.bonree.ants.commons.plugin.PluginGlobals;
import com.bonree.ants.commons.utils.LimitQueue;

import java.util.List;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年5月26日 下午5:24:44
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 插件和schema的管理基类
 * ****************************************************************************
 */
public class ConfigManager {

    /**
     * 概述：保存插件或配置到指定队列
     *
     * @param queue  插件队列
     * @param object 将此对象添加到queue中
     * @return 被替换的对象; 如果是新插入的对象,则返回null
     * @throws Exception
     */
    protected <T> T save(LimitQueue<T> queue, T object) throws Exception {
        long pluginTime = PluginGlobals.getPluginTime(object);
        List<T> etlList = queue.getQueue();
        T tmpEtl;
        int size = etlList.size();
        for (int i = 0; i < size; i++) {
            tmpEtl = etlList.get(i);
            long tmpTime = PluginGlobals.getPluginTime(tmpEtl);
            if (tmpTime == pluginTime) {
                etlList.set(i, object);
                return tmpEtl;
            }
        }
        tmpEtl = size > 0 ? etlList.get(0) : null;
        queue.offer(object);
        return tmpEtl;
    }
}
