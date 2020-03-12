package com.bonree.ants.commons.schema;

import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.plugin.PluginGlobals;
import com.bonree.ants.commons.schema.model.Schema;
import com.bonree.ants.commons.schema.model.SchemaData;
import com.bonree.ants.commons.utils.LimitQueue;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月17日 下午7:54:13
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: ****************************************************************************
 */
public class SchemaGLobals {

    /**
     * schema配置队列
     */
    public static final LimitQueue<Schema> SCHEMA_QUEUE = new LimitQueue<>(GlobalContext.QUEUE_MAX_COUNT);

    /**
     * 维度类型
     */
    public static final String DIMENSION = "dimension";

    /**
     * 隐藏属性
     */
    public static final String HIDDEN = "hidden";

    /**
     * 指标属性
     */
    public static final String METRIC = "metric";

    /**
     * 数据描述配置文件目录
     */
    static final String SCHEMA_DIR = "/conf/schema/";

    /**
     * 扩展置文件目录-基线/报警
     */
    static final String EXTENTION_DIR = "/conf/extention/";

    /**
     * 活跃数配置文件目录
     */
    static final String ACTIVE_DIR = "/conf/active/";

    /**
     * 数据描述配置文件路径
     */
    public static final String SCHEMA_FILE_PATH = "/conf/schema.xml";

    /**
     * 上传配置信息命令
     */
    public static final String UPLOAD = "upload_schema";

    /**
     * 概述：根据业务名称获取schema对象
     *
     * @param time    时间
     * @param bizName 业务名称
     */
    public static SchemaData getSchemaByName(long time, String bizName) {
        Map<String, SchemaData> tmpMap = getSchemaInfo(time);
        if (tmpMap == null) {
            return null;
        }
        if (tmpMap.containsKey(bizName)) {
            return tmpMap.get(bizName);
        }
        return null;
    }

    /**
     * 概述：根据业务类型获取schema对象
     *
     * @param time    时间
     * @param bizType 业务类型(时序业务或基线业务或报警业务)
     */
    public static Map<String, SchemaData> getSchemaByType(long time, String... bizType) {
        Map<String, SchemaData> schemaMap = new HashMap<>();
        if (bizType == null) {
            return schemaMap;
        }
        Map<String, SchemaData> tmpMap = getSchemaInfo(time);
        if (tmpMap == null) {
            return schemaMap;
        }
        for (Entry<String, SchemaData> entry : tmpMap.entrySet()) {
            for (String type : bizType) {
                if (type.equals(entry.getValue().getBizType()) && entry.getValue().isGranule()) {
                    schemaMap.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return schemaMap;
    }

    /**
     * 概述：获取schema对象集合
     *
     * @param time 时间
     */
    private static Map<String, SchemaData> getSchemaInfo(long time) {
        Schema schema = PluginGlobals.get(time, SCHEMA_QUEUE);
        if (schema == null) {
            return null;
        }
        return schema.getData();
    }
}
