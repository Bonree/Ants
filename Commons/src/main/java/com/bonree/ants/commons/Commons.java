package com.bonree.ants.commons;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.bonree.ants.commons.config.AntsConfig;
import com.bonree.ants.commons.schema.SchemaConfigImpl;
import com.bonree.ants.commons.schema.SchemaManager;
import com.bonree.ants.commons.schema.model.Schema;
import com.bonree.ants.commons.utils.DateFormatUtils;
import com.bonree.ants.commons.zookeeper.ZKCommons;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018/11/30 16:35
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: ants公共方法
 * *****************************************************************************
 */
public class Commons {

    private static final Logger log = LoggerFactory.getLogger(Commons.class);

    /**
     * 概述：初始化配置信息
     *
     * @param conf 配置信息
     */
    @SuppressWarnings({"rawtypes"})
    public static void initAntsConfig(Map conf) {
        try {
            // 初始全局配置参数
            Object antsConfig = conf.get(AntsConfig.ANTS_CONFIG);
            if (antsConfig != null) {
                GlobalContext.setConfig(JSON.parseObject(antsConfig.toString(), new TypeReference<AntsConfig>() {
                }));
            }
        } catch (Exception ex) {
            log.error("Init ants config error! ", ex);
        }
    }

    /**
     * 概述：初始化配置信息
     *
     * @param conf ants配置信息
     */
    public static void initAntsSchema(Map conf) {
        try {
            // 1.初始全局配置参数
            initAntsConfig(conf);

            // 2.初始schema配置信息
            byte[] schemaBytes = ZKCommons.instance.getData(GlobalContext.getConfig().getSchemaConfigPath());
            Schema schema = JSON.parseObject(new String(schemaBytes, StandardCharsets.UTF_8), Schema.class);
            SchemaManager manager = new SchemaConfigImpl(schema);
            manager.saveSchema(); // 保存schema信息
        } catch (Exception ex) {
            log.error("Init ants schema error! ", ex);
        }
    }

    /**
     * 概述：获取间隔的时间戳
     *
     * @param time     时间戳
     * @param interval 间隔
     * @return
     */
    public static Date getIntervalTime(long time, int interval) {
        if (time == 0) {
            time = System.currentTimeMillis();
        }
        int tmpTime = DateFormatUtils.getSeconds(time);
        int value = tmpTime / interval * interval;
        return DateFormatUtils.setSeconds(time, value);
    }

    /**
     * 概述：克隆
     *
     * @param obj 克隆对象
     */
    @SuppressWarnings("unchecked")
    public static <T extends Serializable> T clone(T obj) {
        T cloneObj = null;
        try {
            // 写入字节流
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream obs = new ObjectOutputStream(out);
            obs.writeObject(obj);
            obs.close();

            // 分配内存，写入原始对象，生成新对象
            ByteArrayInputStream ios = new ByteArrayInputStream(out.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(ios);
            // 返回生成的新对象
            cloneObj = (T) ois.readObject();
            ois.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return cloneObj;
    }

    /**
     * 概述：set集合排序
     *
     * @param sets set集合
     * @return 升序的list集合
     */
    public static List<String> sortSet(Set<String> sets) {
        List<String> sortList = new ArrayList<>(sets);
        Collections.sort(sortList, new Comparator<String>() {
            @Override
            public int compare(String t1, String t2) {
                return t1.compareTo(t2);
            }
        });
        return sortList;
    }
}
