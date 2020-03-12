package com.bonree.ants.commons.enums;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @Date: 2018年8月20日 下午4:03:55
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 业务类型
 *****************************************************************************
 */
public enum BizType {
    
    NORMAL("normal"),       // 时序指标
    ALERT("alert"),         // 报警
    BASELINE("baseline"),   // 基线
    ACTIVE("active");       // 日活

    private String type;

    BizType(String type) {
        this.type = type;
    }

    public String type() {
        return type;
    }
}
