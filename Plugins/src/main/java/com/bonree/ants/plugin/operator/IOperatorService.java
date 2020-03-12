package com.bonree.ants.plugin.operator;

import com.bonree.ants.plugin.etl.model.Records;

import java.io.Serializable;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @Date: 2018年5月29日 下午3:32:07
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 自定义算子
 *****************************************************************************
 */
public interface IOperatorService extends Serializable {

    /**
     * 概述：自定义算子
     * @param record 一行数据
     * @param classs 数据类型
     * @return 计算结果
     */
    <T> T udf(Records<?, ?> record, T classs) throws Exception;
    <T> T udf1(Records<?, ?> record, T classs) throws Exception;
    <T> T udf2(Records<?, ?> record, T classs) throws Exception;
    <T> T udf3(Records<?, ?> record, T classs) throws Exception;
    <T> T udf4(Records<?, ?> record, T classs) throws Exception;
    <T> T udf5(Records<?, ?> record, T classs) throws Exception;
}
