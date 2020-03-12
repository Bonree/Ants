package com.bonree.ants.storage.quartz;

import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.storage.utils.DBUtils;
import com.bonree.ants.commons.zookeeper.ZKCommons;
import com.bonree.ants.commons.zookeeper.ZKGlobals;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月24日 下午8:38:42
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 检查数据库连接状态
 * ****************************************************************************
 */
public class CheckDBJob implements Job {

    private static final Logger log = LoggerFactory.getLogger(CheckDBJob.class);

    static {
        if (!GlobalContext.getConfig().useStoragePlugin()) { // 不使用插件时加载此参数
            DBUtils.init();
        }
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        if (GlobalContext.getConfig().useStoragePlugin()) {
            return;// 使用插件时不做db连接的检查
        }
        Connection conn = null;
        try {
            conn = DBUtils.getConnection(false);// 获取数据库连接
            if (CheckFlag.getdbFlag()) {
                ZKCommons.instance.setData(ZKGlobals.DB_MONITOR_PATH, ZKGlobals.FALSE);
            }
            CheckFlag.setdbFlag(false);
        } catch (Exception ex) {
            CheckFlag.setdbFlag(true);
            ZKCommons.instance.setData(ZKGlobals.DB_MONITOR_PATH, ZKGlobals.TRUE);
            log.error("Check db connetion error! {}", ex.getMessage());
        } finally {
            DBUtils.close(null, null, conn);
        }
    }
}
