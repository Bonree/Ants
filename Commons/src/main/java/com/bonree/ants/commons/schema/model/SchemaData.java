package com.bonree.ants.commons.schema.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月26日 下午6:48:23
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: Schema的每个业务实例类
 * ****************************************************************************
 */
public class SchemaData implements Serializable {

    private static final long serialVersionUID = 1L;

    private String bizName; // 业务名称(对应数据表名称)

    private long part;    // 数据表的分区号(用于一个表入多个维度)

    private String bizType; // 业务类型, 时序业务或基线业务或报警业务

    private String bizSql;  // 业务sql

    private boolean isGranule = true; // 是否需要进行粒度汇总,默认需要汇总

    private List<SchemaFields> fieldsList = new ArrayList<>(); // 数据属性集合

    public String getBizName() {
        return bizName;
    }

    public void setBizName(String bizName) {
        this.bizName = bizName;
    }

    public long getPart() {
        return part;
    }

    public void setPart(long part) {
        this.part = part;
    }

    public String getBizType() {
        return bizType;
    }

    public void setBizType(String bizType) {
        this.bizType = bizType;
    }

    public String getBizSql() {
        return bizSql;
    }

    public void setBizSql(String bizSql) {
        this.bizSql = bizSql;
    }

    public void addFields(SchemaFields schema) {
        this.fieldsList.add(schema);
    }

    public void setFieldsList(List<SchemaFields> fieldsList) {
        this.fieldsList = fieldsList;
    }

    public List<SchemaFields> getFieldsList() {
        return fieldsList;
    }

    public boolean isGranule() {
        return isGranule;
    }

    public void setGranule(boolean granule) {
        isGranule = granule;
    }
}
