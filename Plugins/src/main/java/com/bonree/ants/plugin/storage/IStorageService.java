package com.bonree.ants.plugin.storage;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;

import com.bonree.ants.plugin.etl.model.CustomData;
import com.bonree.ants.plugin.storage.model.DataResult;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年5月2日 下午6:23:06
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 数据存储接口
 * ****************************************************************************
 */
public interface IStorageService extends Serializable {

    /**
     * 概述：统计数据存储接口
     *
     * @param content 数据内容
     * @throws SQLException 抛出此异常时,计算框架将会包数据重新发回kafka
     * @throws Exception    抛出异常时,计算框架将会保存此包数据在本地磁盘
     */
    void message(DataResult content) throws SQLException, Exception;

    /**
     * 概述：自定义数据存储接口
     *
     * @param customData 数据内容
     * @throws Exception 抛出异常时,计算框架将会保存此包数据在本地磁盘
     */
    void message(CustomData customData) throws Exception;
}
