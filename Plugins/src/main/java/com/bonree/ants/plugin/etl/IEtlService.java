package com.bonree.ants.plugin.etl;

import com.bonree.ants.plugin.etl.model.DataSet;
import com.bonree.ants.plugin.etl.model.SchemaConfig;

import java.io.Serializable;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月8日 下午6:19:20
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 数据etl接口
 * ****************************************************************************
 */
public interface IEtlService extends Serializable {

    /**
     * 概述：初始化参数
     *
     * @param obj 自定义对象
     * @throws Exception
     */
    void init(Object obj) throws Exception;

    /**
     * 概述：数据处理接口
     *
     * @param content 源数据内容
     * @param config  schema配置信息
     * @return etl后的数据对象
     */
    DataSet onMessage(byte[] content, SchemaConfig config) throws Exception;

    /**
     * 概述：释放资源
     *
     * @throws Exception
     */
    void close() throws Exception;
}
