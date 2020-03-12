package com.bonree.ants.extention.topology.commons;

import com.bonree.ants.commons.config.ParamsConfig;

/**
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018/10/18 15:39
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 扩展拓扑启动参数
 * *****************************************************************************
 */
public class ExtentionParams<T> extends ParamsConfig<T> {

    /**
     * 概述：数据预汇总线程数
     */
    private String agg_num = "an";

    public ExtentionParams() {
        super();
        this.put(agg_num, null);
    }

    public T getAggNum() {
        return this.get(agg_num);
    }

    public void setAggNum(T an) {
        this.put(agg_num, an);
    }
}
