package com.bonree.ants.plugin.etl.model;

import com.bonree.ants.plugin.storage.model.DataResult;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月8日 下午4:22:31
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: ETL后的数据集合
 *****************************************************************************
 */
public class DataSet implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 数据包的时间戳
     */
    private long timestamp;

    /**
     * 需要直接存储的结果数据
     * KEY:   业务名称
     * VALUE: 当前业务下的数据集合
     */
    private Map<String, DataResult> result = new HashMap<>();

    /**
     * 自定义数据
     */
    private CustomData customData;

    /**
     * 封装后的指标数据集合
     * KEY:   业务名称
     * VALUE: 当前业务下的源数据集合
     */
    private Map<String, List<Records<String, Object>>> data = new HashMap<>();

    /**
     * 概述：添加一条记录
     *
     * @param bizName 业务名称
     * @param record  一条数据记录
     */
    public void addRecords(String bizName, Records<String, Object> record) {
        if (!data.containsKey(bizName)) {
            data.put(bizName, new ArrayList<Records<String, Object>>());
        }
        data.get(bizName).add(record);
    }

    /**
     * 概述：获取当前数据包的所有记录
     */
    public Map<String, List<Records<String, Object>>> get() {
        return data;
    }

    /**
     * 概述：获取当前业务的所有记录
     *
     * @param bizName 业务名称
     */
    public List<Records<String, Object>> get(String bizName) {
        return data.get(bizName);
    }

    /**
     * 概述：是否包含此业务
     *
     * @param bizName
     */
    public boolean containsKey(String bizName) {
        return data.containsKey(bizName);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long time) {
        this.timestamp = time;
    }

    public CustomData getCustomData() {
        return customData;
    }

    public void setCustomData(CustomData customData) {
        this.customData = customData;
    }

    public Map<String, DataResult> getResult() {
        return result;
    }

    public void setResult(Map<String, DataResult> result) {
        this.result = result;
    }

    public String toString() {
        StringBuilder sb = null;
        for (Entry<String, List<Records<String, Object>>> entry : data.entrySet()) {
            if (sb == null) {
                sb = new StringBuilder("{");
            } else {
                sb.append(", ");
            }
            sb.append(entry.getKey());
            sb.append(":[");
            for (Records<?, ?> re : entry.getValue()) {
                sb.append(re);
            }
            sb.append("]");
        }
        return sb == null ? "" : sb.append("}").toString();
    }
}
