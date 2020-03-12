package com.bonree.ants.calc.utils;

/**
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2019/1/29 15:23
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: TODO
 * *****************************************************************************
 */
public class Globals {

    /**
     * 概述：当前服务器唯一的编号
     */
    private static int localUniqueNo;

    public static int getLocalUniqueNo(){
        return localUniqueNo;
    }

    public static void setLocalUniqueNo(int localUniqueNo){
        Globals.localUniqueNo = localUniqueNo;
    }


}
