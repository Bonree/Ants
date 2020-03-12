package com.bonree.ants.commons.schema.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @Date: 2018年5月7日 下午8:23:22
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: Schema每个业务的数据字段类
 *****************************************************************************
 */
public class SchemaFields implements Serializable {

    private static final long serialVersionUID = 1L;

    private String source;      // 数据源中的属性名称
    private String name;        // 属性名称
    private String valueType;   // 属性类型, long, double, string, timestamp
    private String expr;        // 计算函数, 支持:sum,max,min,自定义函数:(a+b)*c
    private String type;        // 属性标识(取值:dimension和metric)
    private List<String> expressList = new ArrayList<>(); // 当前属性所需要的表达式属性

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

    public String getValueType() {
        return valueType;
    }

    public void setValueType(String valueType) {
        this.valueType = valueType;
    }

    public String getExpr() {
        return expr;
    }

    public void setExpr(String expr) {
        this.expr = expr;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setExpressList(List<String> expressList) {
        this.expressList = expressList;
    }

    public List<String> getExpressList() {
        return expressList;
    }

}
