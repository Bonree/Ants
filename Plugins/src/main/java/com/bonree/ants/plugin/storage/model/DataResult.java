package com.bonree.ants.plugin.storage.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.bonree.ants.plugin.etl.model.Records;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月11日 上午9:57:44
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 数据计算结果
 * ****************************************************************************
 */
public class DataResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private String bizName; // 业务表名称
    private String bizType; // 业务表类型, 正常业务或基线基线业务
    private int granule;    // 粒度,快照表数据时不用赋值.
    private String time;    // 当前数据包的时间(yyyyMMddHHmmss)
    private int count = 0;      // 当前数据包属性"recordList"的数据集合条数
    private List<Records<String, Object>> recordList = new ArrayList<>(); // 数据集合

    /**
     * 概述：添加数据
     *
     * @param record 数据对象
     */
    public void add(Records<String, Object> record) {
        recordList.add(record);
    }

    /**
     * 概述：添加数据
     *
     * @param list 数据对象集合
     */
    public void addAll(Collection<Records<String, Object>> list) {
        recordList.addAll(list);
    }

    /**
     * 概述：获取数据
     *
     * @return
     */
    public List<Records<String, Object>> get() {
        return recordList;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public int getGranule() {
        return granule;
    }

    public void setGranule(int granule) {
        this.granule = granule;
    }

    public String getBizName() {
        return bizName;
    }

    public void setBizName(String bizName) {
        this.bizName = bizName;
    }

    public String getBizType() {
        return bizType;
    }

    public void setBizType(String type) {
        this.bizType = type;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
