package com.bonree.ants.commons.config;

import java.util.HashMap;

/**
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018/10/18 9:45
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 拓扑启动参数
 * *****************************************************************************
 */
public class ParamsConfig<T> extends HashMap<String, T> {

    /**
     * 概述：拓扑worker线程数
     */
    private String worker_num = "wn";

    protected ParamsConfig() {
        this.put(worker_num, null);
    }

    public T getWorkerNum() {
        return this.get(worker_num);
    }

    public void setWorkerNum(T workerNum) {
        this.put(worker_num, workerNum);
    }

}
