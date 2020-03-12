package com.bonree.ants.storage.commons;

import com.bonree.ants.commons.config.ParamsConfig;

/**
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018/10/18 15:25
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 存储拓扑启动参数
 * *****************************************************************************
 */
public class StorageParams<T> extends ParamsConfig<T> {

    /**
     * 概述：spout并发数
     */
    private String spout_pending_num = "spn";

    /**
     * 概述：spout线程数
     */
    private String spout_thread_num = "stn";

    /**
     * 概述：数据处理线程数
     */
    private String shuffle_num = "sn";

    public StorageParams() {
        super();
        this.put(spout_pending_num, null);
        this.put(spout_thread_num, null);
        this.put(shuffle_num, null);
    }

    public T getSpoutPendingNum() {
        return this.get(spout_pending_num);
    }

    public void setSpoutPendingNum(T spn) {
        this.put(spout_pending_num, spn);
    }

    public T getSpoutThreadNum() {
        return this.get(spout_thread_num);
    }

    public void setSpoutThreadNum(T stn) {
        this.put(spout_thread_num, stn);
    }

    public T getShuffleNum() {
        return this.get(shuffle_num);
    }

    public void setShuffleNum(T sn) {
        this.put(shuffle_num, sn);
    }
}
