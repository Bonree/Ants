package com.bonree.ants.commons.engine;

import com.alibaba.fastjson.JSON;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.config.AntsConfig;
import com.bonree.ants.commons.enums.Delim;
import com.bonree.ants.commons.granule.model.Granule;
import com.bonree.ants.commons.granule.model.GranuleMap;
import com.bonree.ants.commons.redis.JedisClient;
import com.bonree.ants.commons.schema.SchemaGLobals;
import com.bonree.ants.commons.utils.FileUtils;
import com.bonree.ants.commons.utils.XmlParseUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.Config;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年8月31日 上午11:31:30
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: ants引擎拓扑主服务
 * ****************************************************************************
 */
public abstract class EngineServer {

    private static final Logger log = LoggerFactory.getLogger(EngineServer.class);

    /**
     * 概述：全局配置信息
     */
    protected AntsConfig<Object> antsConfig = new AntsConfig<>();

    /**
     * 概述：拓扑启动内存
     */
    protected String topologyMemory;

    /**
     * 概述：程序的Home目录
     */
    protected String antsHome;

    /**
     * 概述： 插件的Home目录
     */
    private String pluginHome;

    /**
     * 概述： 拓扑启动时存储插件订阅的数据topic
     */
    protected String storageTopic = "";

    /**
     * 概述：加载应用配置信息
     *
     * @param appConfig app配置文件对象
     */
    protected abstract void loadAppInfo(XmlParseUtils appConfig) throws Exception;

    /**
     * 概述：启动拓扑
     *
     * @param config storm参数配置对象
     */
    protected abstract void startup(Config config) throws Exception;

    /**
     * 概述：加载业务描述信息
     */
    protected void loadSchemaInfo() throws Exception {
    }

    public void install(String[] args) {
        try {
            String topoName = "";
            if (args != null && args.length >= 1) {// 拓扑名称
                topoName = args[0].trim();
            }
            if (args != null && args.length >= 2) {// 程序根路径
                antsHome = args[1].trim();
            }
            if (args != null && args.length >= 3) {// 存储拓扑订阅kafka的topic
                storageTopic = args[2].trim();
            }
            if (StringUtils.isEmpty(storageTopic)) {
                antsConfig.setTopologyName(topoName);
            } else {
                antsConfig.setTopologyName(topoName + "_" + storageTopic);
            }
            pluginHome = antsHome + "/plugin";
            XmlParseUtils appConfig = new XmlParseUtils(new File(antsHome + GlobalContext.APP_CONFIG_PATH)); // app配置文件对象

            // 1.加载应用配置信息
            loadBaseInfo(appConfig);

            // 2.上传schema文件和app配置到zk
            if (SchemaGLobals.UPLOAD.equals(antsConfig.getTopologyName())) {
                uploadSchemaInfo(appConfig);
                log.info("upload schema to zookeeper complete!");
                return;
            }

            // 3.加载app相关配置信息
            loadAppInfo(appConfig);

            // 4.加载业务描述信息
            loadSchemaInfo();

            // 5.启动启动拓扑
            Config config = new Config();
            config.put(AntsConfig.ANTS_CONFIG, JSON.toJSONString(antsConfig)); // 添加自定义配置
            startup(config);
        } catch (Exception ex) {
            log.error("Startup {} error!", GlobalContext.getConfig().getTopologyName(), ex);
        }
    }

    /**
     * 概述：加载基础配置信息
     */
    private void loadBaseInfo(XmlParseUtils config) throws Exception {
        // 引擎命名空间
        Element element = config.getElement("ants");
        antsConfig.setNamespace(config.getAttr(element, "namespace"));
        // zookeeper信息
        Element zookeeperElement = config.getElement("zookeeper");
        antsConfig.setZookeeperCluster(config.getListText(zookeeperElement, "zookeeper.cluster"));
        // redis信息
        Element redisElement = config.getElement("redis");
        antsConfig.setRedisCluster(config.getListText(redisElement, "redis.cluster"));
        // kafka信息
        Element kafkaElement = config.getElement("kafka");
        antsConfig.setBootstrapServers(config.getListText(kafkaElement, "kafka.cluster"));
        // 计算公共信息
        Element globalElement = config.getElement("global");
        antsConfig.setRootPath(config.getStringValue(globalElement, "root.path"));
        antsConfig.setSupervisorCount(config.getIntegerValue(globalElement, "storm.supervisor.count"));
        antsConfig.setSourceGranule(config.getIntegerValue(globalElement, "source.granule"));
        GlobalContext.setConfig(antsConfig);
    }

    /**
     * 概述：上传配置信息到zk(由子类实现)
     *
     * @param config 配置文件对象
     * @throws Exception
     */
    protected void uploadSchemaInfo(XmlParseUtils config) throws Exception {

    }

    /**
     * 概述：初始粒度的相关属性
     *
     * @param config          配置文件对象
     * @param element         粒度的配置信息所在的元素对象
     * @param currGranuleSets 当前拓扑的粒度集合
     * @throws Exception
     */
    protected void initGranuleInfo(XmlParseUtils config, Element element, Set<Integer> currGranuleSets) throws Exception {
        Integer[] granuleArr = config.getIntArray(element, "granule", Delim.COMMA.value());
        Integer[] redisBucketArr = config.getIntArray(element, "redis.bucket.count", Delim.COMMA.value());
        Integer[] calcParallelArr = config.getIntArray(element, "granule.calc.parallel", Delim.COMMA.value());
        Integer[] granuleTimeoutArr = config.getIntArray(element, "granule.timeout", Delim.COMMA.value());
        Integer[] skipStorageArr = config.getIntArray(element, "skip.storage.granule", Delim.COMMA.value());
        Set<Integer> skipSets = new HashSet<>(Arrays.asList(skipStorageArr));
        Granule granule;
        GranuleMap granuleMap = new GranuleMap();
        for (int i = 0; i < granuleArr.length; i++) { // 处理粒度相关属性
            int curGra = granuleArr[i];
            if (currGranuleSets != null && !currGranuleSets.contains(curGra)) {
                continue;
            }
            granule = new Granule();
            granule.setGranule(curGra); // 当前粒度
            granule.setBucket(redisBucketArr[i]); // 当前粒度保存到redis时的分桶数量
            granule.setLastBucket(i == 0 ? redisBucketArr[i] : redisBucketArr[i - 1]); // 当前粒度保存到redis时上一个粒度的分桶数量
            granule.setNextBucket(i >= granuleArr.length - 1 ? redisBucketArr[i] : redisBucketArr[i + 1]); // 当前粒度保存到redis时上一个粒度的分桶数量
            granule.setParallel(antsConfig.getSupervisorCount() * calcParallelArr[i]); // 当前粒度在汇总时的并行度
            granule.setTimeout(granuleTimeoutArr[i] * 60 * 1000); // 当前粒度在汇总时等待的超时时间
            granule.setLast(i == 0 ? antsConfig.getSourceGranule() : granuleArr[i - 1]); // 当前粒度的上一个粒度
            granule.setNext(i >= granuleArr.length - 1 ? granuleArr[i] : granuleArr[i + 1]); // 当前粒度的下一下粒度
            granule.setSkip(skipSets.contains(curGra)); // 当前粒度是否不需要进行存储, true 需要; false 不需要
            granuleMap.put(curGra, granule);
        }
        antsConfig.setGranuleMap(JSON.toJSONString(granuleMap));
        antsConfig.setFirstGranule(granuleArr[0]);
        antsConfig.setTailGranule(granuleArr[granuleArr.length - 1]);
    }

    /**
     * 保存插件到redis
     *
     * @param config            配置文件对象
     * @param pluginEle         插件元素对象
     * @param pluginKey         插件保存redis中的key
     * @param pluginPathArrName 获取插件元素信息的属性名称
     */
    protected void savePluginToRedis(XmlParseUtils config, Element pluginEle, String pluginKey, String pluginPathArrName) {
        String pluginPath = pluginHome + config.getStringValue(pluginEle, pluginPathArrName);
        byte[] storageByte = FileUtils.file2Byte(pluginPath);
        JedisClient.setByKey(pluginKey, storageByte); // 在key上加topic是为了区分不同插件
        log.info("Set plugin to redis. arrName:{}, length: {}, path: {}", pluginPathArrName, storageByte.length, pluginPath);
    }
}
