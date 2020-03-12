package com.bonree.ants.calc.topology.commons;

import com.bonree.ants.commons.config.ParamsConfig;

/**
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018/10/18 10:09
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 预处理拓扑启动参数
 * *****************************************************************************
 */
public class PreprocessParams<T> extends ParamsConfig<T> {

    /**
     * 概述：spout并发数
     */
    private String spout_pending_num = "spn";

    /**
     * 概述：spout线程数
     */
    private String spout_thread_num = "stn";

    /**
     * 概述：预处理拓扑批量处理数据的数量
     */
    private String batch_num = "bn";

    /**
     * 概述：数据预处理线程数
     */
    private String shuffle_num = "sn";

    /**
     * 概述：全局汇总线程数
     */
    private String group_num = "gn";

    public PreprocessParams() {
        super();
        this.put(spout_pending_num, null);
        this.put(spout_thread_num, null);
        this.put(batch_num, null);
        this.put(shuffle_num, null);
        this.put(group_num, null);
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

    public T getBatchNum() {
        return this.get(batch_num);
    }

    public void setBatchNum(T bn) {
        this.put(batch_num, bn);
    }

    public T getShuffleNum() {
        return this.get(shuffle_num);
    }

    public void setShuffleNum(T sn) {
        this.put(shuffle_num, sn);
    }

    public T getGroupNum() {
        return this.get(group_num);
    }

    public void setGroupNum(T gn) {
        this.put(group_num, gn);
    }

}
