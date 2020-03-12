package com.bonree.ants.commons;

import java.sql.Timestamp;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @Date: 2018年4月11日 上午10:28:42
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 数据聚合类型
 *****************************************************************************
 */
public enum AggType {

    // 数据类型
    LONG("long"),
    DOUBLE("double"),
    TIMESTAMP("timestamp"), 
    STRING("string"), 
    
    // 计算类型
    MAX("max"), 
    MAXIF("maxif"), 
    MIN("min"), 
    MINIF("minif"), 
    SUM("sum"), 
    SUMIF("sumif"), 
    COUNT("count"),
    COUNTIF("countif"),
    UDF("udf"),
    UDF1("udf1"),
    UDF2("udf2"),
    UDF3("udf3"),
    UDF4("udf4"),
    UDF5("udf5"),
    MEDIAN("median"), // 50分位中位数
    MEDIAN90("median90"), // 90分位中位数
    HYPERLOG("hyperlog"), // 去重函数,用于计算去掉重复数据后的条数
    
    // 特殊标识
    IF("if");
    

    private String type;

    AggType(String type) {
        this.type = type;
    }

    public String type() {
        return type;
    }
    
    @SuppressWarnings("unchecked")
    public static <T> T convert(String type) {
        if (LONG.name().equals(type)) {
            return (T) Long.class;
        } else if (DOUBLE.name().equals(type)) {
            return (T) Double.class;
        } else if (TIMESTAMP.name().equals(type)) {
            return (T) Timestamp.class;
        } else if (STRING.name().equals(type)) {
            return (T) String.class;
        }
        return null;
    }
}
