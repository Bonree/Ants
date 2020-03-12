package com.bonree.ants.commons.schema.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年5月9日 上午11:36:12
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: schema配置信息实体类
 * ****************************************************************************
 */
public class Schema implements Serializable {

    private static final long serialVersionUID = 1L;

    private long createTime; // schema配置信息生成的时间

    private String type; // 源数据格式: 目前支持xml和json两种

    private Map<String, SchemaData> data = new HashMap<>(); // schema的业务集合,<业务名称, 业务信息>

    private Map<String, String> relation = new HashMap<>(); // 业务数据属性赋初值时与原始数据属性的关联关系<业务数据的属性名称, 原始数据属性的路径>

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void putData(SchemaData data) {
        this.data.put(data.getBizName(), data);
    }

    public void setData(Map<String, SchemaData> data) {
        this.data = data;
    }

    public SchemaData getData(String bizName) {
        return data.get(bizName);
    }

    public Map<String, SchemaData> getData() {
        return data;
    }

    public Map<String, String> getRelation() {
        return relation;
    }

    public void putRelation(String name, String path) {
        this.relation.put(name, path);
    }

    public void setRelation(Map<String, String> relation) {
        this.relation = relation;
    }

    public String getRelation(String name) {
        return this.relation.get(name);
    }
}
