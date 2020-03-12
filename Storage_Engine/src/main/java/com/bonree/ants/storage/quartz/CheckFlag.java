package com.bonree.ants.storage.quartz;

/**
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2019/1/29 15:41
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 检查标记
 * *****************************************************************************
 */
public class CheckFlag {

    /**
     * 数据库发生异常的标记
     */
    private static volatile boolean DB_FLAG = true;

    public static boolean getdbFlag() {
        return DB_FLAG;
    }

    public static void setdbFlag(boolean dbFlag) {
        CheckFlag.DB_FLAG = dbFlag;
    }

}
