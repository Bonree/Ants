package com.bonree.ants.plugin.etl.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @Date: 2018年7月12日 上午10:35:58
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: Schema配置文件信息
 *****************************************************************************
 */
public class SchemaConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 概述：数据来源的格式,目前支持xml和json两种
     */
    private String type;

    /**
     * 概述：schema的业务集合,<数据表名称, 数据列集合>
     */
    private Map<String, List<Field>> config = new HashMap<>();

    /**
     * 概述：业务数据属性赋初值时与原始数据属性的关联关系<业务数据的属性名称, 原始数据属性的路径>
     */
    private Map<String, String> relation = new HashMap<>();

    public void addConfig(String tableName, Field field) {
        if (!this.config.containsKey(tableName)) {
            this.config.put(tableName, new ArrayList<Field>());
        }
        this.config.get(tableName).add(field);
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setConfig(Map<String, List<Field>> config) {
        this.config = config;
    }

    public List<Field> getConfig(String tableName) {
        return config.get(tableName);
    }

    public Map<String, List<Field>> getConfig() {
        return config;
    }

    public Map<String, String> getRelation() {
        return relation;
    }

    public void setRelation(Map<String, String> relation) {
        this.relation = relation;
    }

    public void putRelation(String name, String path) {
        this.relation.put(name, path);
    }

    public String getRelation(String name) {
        return this.relation.get(name);
    }

    /**
     * schema业务表的列对象
     */
    public static class Field implements Serializable {
        private String source; // 数据源中的属性名称
        private String name;   // 属性名称
        private String type;   // 属性类型, long, double, string, timestamp

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }
}
