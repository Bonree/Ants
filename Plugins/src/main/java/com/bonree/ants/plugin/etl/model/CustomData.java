package com.bonree.ants.plugin.etl.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年8月1日 下午4:31:39
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 自定义数据
 * ****************************************************************************
 */
public class CustomData implements Serializable {

    /**
     * 初始时间
     */
    private long time;

    /**
     * 自定义数据集合
     */
    private List<File> dataList = new ArrayList<>();

    public CustomData() {

    }

    public CustomData(long time) {
        this.time = time;
    }

    /**
     * 数据文件对象
     */
    public static class File implements Serializable {
        private String contextType; // 上下文类型，一般为topic
        private byte[] context; // 上下文信息
        private byte[] content; // 数据内容

        public String getContextType() {
            return contextType;
        }

        public void setContextType(String contextType) {
            this.contextType = contextType;
        }

        public byte[] getContext() {
            return context.clone();
        }

        public void setContext(byte[] context) {
            this.context = context.clone();
        }

        public byte[] getContent() {
            return content.clone();
        }

        public void setContent(byte[] content) {
            this.content = content.clone();
        }
    }

    /**
     * 概述：添加一条记录
     *
     * @param file 自定义数据
     */
    public void addData(File file) {
        dataList.add(file);
    }

    public List<File> getDataList() {
        return dataList;
    }

    public void setDataList(List<File> dataList) {
        this.dataList = dataList;
    }

    public long getTime() {
        return time;
    }
}
