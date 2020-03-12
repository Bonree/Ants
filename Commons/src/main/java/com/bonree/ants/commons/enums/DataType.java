package com.bonree.ants.commons.enums;

import java.sql.Timestamp;

/**
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月11日 上午10:28:42
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 数据类型
 * *****************************************************************************
 */
public enum DataType {


    // 数据类型
    LONG("long"),
    DOUBLE("double"),
    TIMESTAMP("timestamp"),
    STRING("string");


    private String type;

    DataType(String type) {
        this.type = type;
    }

    public String type() {
        return type;
    }

    /**
     * 转换数据类型为Java类型
     */
    @SuppressWarnings("unchecked")
    public <T> T tclass() {
        switch (this) {
            case DOUBLE:
                return (T) Double.class;
            case LONG:
                return (T) Long.class;
            case TIMESTAMP:
                return (T) Timestamp.class;
            case STRING:
                return (T) String.class;
            default:
                throw new IllegalArgumentException("Illegal character type: " + this.type);
        }
    }

    /**
     * 转换数据
     *
     * @param value 数据内容
     */
    @SuppressWarnings("unchecked")
    public <T> T convert(Object value) {
        switch (this) {
            case DOUBLE:
                return (T) (value == null || "".equals(value) ? new Double(0.0) : value instanceof Double ? (Double) value : Double.valueOf(value.toString()));
            case LONG:
            case TIMESTAMP: // 时间戳按long处理.
                return (T) (value == null || "".equals(value) ? new Long(0) : value instanceof Long ? (Long) value : Long.valueOf(value.toString()));
            case STRING:
                return (T) (value == null ? "" : value.toString());
            default:
                throw new IllegalArgumentException("Illegal character type: " + this.type);
        }
    }
}
