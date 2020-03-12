package com.bonree.ants.plugin.etl.model;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月8日 下午4:18:48
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 一行记录
 * ****************************************************************************
 */
public class Records<K, V> implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 有序的数据属性集合
     * KEY:   属性名称
     * VALUE: 属性值
     */
    private Map<K, V> records = new LinkedHashMap<>();

    public Map<K, V> getRecords() {
        return records;
    }

    /**
     * 概述：添加记录的属性
     *
     * @param name  属性名称
     * @param value 属性值
     */
    public void put(K name, V value) {
        records.put(name, value);
    }

    /**
     * 概述：获取一条记录
     *
     * @return 一条记录的所有属性信息
     */
    public Map<K, V> get() {
        return getRecords();
    }

    /**
     * 概述：获取一条记录中的一个属性信息
     *
     * @param name 属性名称
     * @return 属性值
     */
    public V get(K name) {
        return records.get(name);
    }

    /**
     * 概述：是否包含此属性
     *
     * @param name
     */
    public boolean containsKey(K name) {
        return records.containsKey(name);
    }

    public String toString() {
        if (records.isEmpty()) {
            return "";
        }
        StringBuilder sb = null;
        for (Entry<K, V> entry : records.entrySet()) {
            if (sb == null) {
                sb = new StringBuilder();
            } else {
                sb.append(", ");
            }
            sb.append("{");
            sb.append(entry.getKey());
            sb.append(":");
            sb.append(entry.getValue());
            sb.append("}");
        }
        return sb.toString();
    }

    public String toValueString() {
        if (records.isEmpty()) {
            return "";
        }
        StringBuilder sb = null;
        for (Entry<K, V> entry : records.entrySet()) {
            if (sb == null) {
                sb = new StringBuilder();
            } else {
                sb.append(", ");
            }
            sb.append(entry.getValue());
        }
        return sb.toString();
    }
}
