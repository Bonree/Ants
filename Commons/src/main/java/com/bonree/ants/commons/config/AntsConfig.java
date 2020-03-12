package com.bonree.ants.commons.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月12日 上午11:18:18
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 全局属性集合
 * ****************************************************************************
 */
public class AntsConfig<T> extends HashMap<String, T> {

    /**
     * 概述：全局属性名称
     */
    public final static String ANTS_CONFIG = "ants.coustom.config";

    /**
     * 引擎命名空间
     */
    private final static String NAMESPACE_CONFIG = "namespace.config";

    public String getNamespace() {
        return getString(NAMESPACE_CONFIG);
    }

    public void setNamespace(T namespace) {
        this.put(NAMESPACE_CONFIG, namespace);
    }

    /**
     * 概述：数据根路径
     */
    private final static String ROOT_PATH_CONFIG = "root.path.config";

    public String getRootPath() {
        return getString(ROOT_PATH_CONFIG);
    }

    public void setRootPath(T rootPath) {
        this.put(ROOT_PATH_CONFIG, rootPath);
    }

    /**
     * 概述：storm集群计算节点数量
     */
    private final static String SUPERVISOR_COUNT_CONFIG = "supervisor.count.config";

    public int getSupervisorCount() {
        return getInteger(SUPERVISOR_COUNT_CONFIG);
    }

    public void setSupervisorCount(T supervisorCount) {
        this.put(SUPERVISOR_COUNT_CONFIG, supervisorCount);
    }

    /**
     * 拓扑名称
     */
    private static final String TOPOLOGY_NAME_CONFIG = "custom.topology.name";

    public String getTopologyName() {
        return getString(TOPOLOGY_NAME_CONFIG);
    }

    public void setTopologyName(T topologyName) {
        this.put(TOPOLOGY_NAME_CONFIG, topologyName);
    }

    /**
     * redis集群地址
     */
    private static final String REDIS_CLUSTER_CONFIG = "redis.cluster";

    public String getRedisCluster() {
        return getString(REDIS_CLUSTER_CONFIG);
    }

    public void setRedisCluster(T redisCluster) {
        this.put(REDIS_CLUSTER_CONFIG, redisCluster);
    }

    /**
     * ZOOKEEPER集群地址
     */
    private static final String ZOOKEEPER_CLUSTER_CONFIG = "zookeeper.cluster";

    public String getZookeeperCluster() {
        return getString(ZOOKEEPER_CLUSTER_CONFIG);
    }

    public void setZookeeperCluster(T zookeeperCluster) {
        this.put(ZOOKEEPER_CLUSTER_CONFIG, zookeeperCluster);
    }

    /**
     * 概述：schema配置信息存储的路径
     */
    private static final String SCHAME_CONFIG_PATH = "schame.config.path";

    public String getSchemaConfigPath() {
        return getString(SCHAME_CONFIG_PATH);
    }

    public void setSchemaConfigPath(T schemaConfigPath) {
        this.put(SCHAME_CONFIG_PATH, schemaConfigPath);
    }

    /**
     * 计算结果发送到kafka每包的数据量
     */
    private static final String KAFKA_BATCH_COUNT_CONFIG = "result.kafka.batch.count";

    public int getKafkaBatchCount() {
        return getInteger(KAFKA_BATCH_COUNT_CONFIG);
    }

    public void setKafkaBatchCount(T kafkaBatchCount) {
        this.put(KAFKA_BATCH_COUNT_CONFIG, kafkaBatchCount);
    }

    /**
     * 中间计算结果到redis每包的数据量
     */
    private static final String REDIS_BATCH_COUNT_CONFIG = "result.redis.batch.count";

    public int getRedisBatchCount() {
        return getInteger(REDIS_BATCH_COUNT_CONFIG);
    }

    public void setRedisBatchCount(T redisBatchCount) {
        this.put(REDIS_BATCH_COUNT_CONFIG, redisBatchCount);
    }

    /**
     * 预处理拓扑处理数据时粒度
     */
    private static final String SOURCE_GRANULE_CONFIG = "source.granule";

    public int getSourceGranule() {
        return getInteger(SOURCE_GRANULE_CONFIG);
    }

    public void setSourceGranule(T sourceGranule) {
        this.put(SOURCE_GRANULE_CONFIG, sourceGranule);
    }

    /**
     * ETL插件jar实现接口的完整包路径(包含实现类名称)
     */
    private static final String ETL_SERVICE_IMPL_CONFIG = "etl.service.impl";

    public String getEtlServiceImpl() {
        return getString(ETL_SERVICE_IMPL_CONFIG);
    }

    public void setEtlServiceImpl(T etlServiceImpl) {
        this.put(ETL_SERVICE_IMPL_CONFIG, etlServiceImpl);
    }

    /**
     * ETL插件jar名称key
     */
    private final static String ETL_PLUGIN_NAME_CONFIG = "etl.plugin.name";

    public String getEtlPluginName() {
        return getString(ETL_PLUGIN_NAME_CONFIG);
    }

    public void setEtlPluginName(T etlPluginName) {
        this.put(ETL_PLUGIN_NAME_CONFIG, etlPluginName);
    }

    /**
     * 是否使用入库插件
     */
    private static final String USE_STORAGE_PLUGIN_CONFIG = "use.storage.plugin";

    public boolean useStoragePlugin() {
        return getBoolean(USE_STORAGE_PLUGIN_CONFIG);
    }

    public void setUseStoragePlugin(T useStoragePlugin) {
        this.put(USE_STORAGE_PLUGIN_CONFIG, useStoragePlugin);
    }

    /**
     * 存储插件jar实现接口的完整包路径(包含实现类名称)
     */
    private static final String STORAGE_SERVICE_IMPL_CONFIG = "storage.service.impl";

    public String getStorageServiceImpl() {
        return getString(STORAGE_SERVICE_IMPL_CONFIG);
    }

    public void setStorageServiceImpl(T storageServiceImpl) {
        this.put(STORAGE_SERVICE_IMPL_CONFIG, storageServiceImpl);
    }

    /**
     * OPERATOR插件jar名称key
     */
    private final static String STORAGE_PLUGIN_NAME_CONFIG = "storage.plugin.name";

    public String getStoragePluginName() {
        return getString(STORAGE_PLUGIN_NAME_CONFIG);
    }

    public void setStoragePluginName(T storagePluginName) {
        this.put(STORAGE_PLUGIN_NAME_CONFIG, storagePluginName);
    }

    /**
     * 是否使用OPERATOR插件
     */
    private static final String USE_OPERATOR_PLUGIN_CONFIG = "use.operator.plugin";

    public boolean useOperatorPlugin() {
        return getBoolean(USE_OPERATOR_PLUGIN_CONFIG);
    }

    public void setUseOperatorPlugin(T useOperatorPlugin) {
        this.put(USE_OPERATOR_PLUGIN_CONFIG, useOperatorPlugin);
    }

    /**
     * OPERATOR插件jar实现接口的完整包路径(包含实现类名称)
     */
    private static final String OPERATOR_SERVICE_IMPL_CONFIG = "operator.service.impl";

    public String getOperatorServiceImpl() {
        return getString(OPERATOR_SERVICE_IMPL_CONFIG);
    }

    public void setOperatorServiceImpl(T operatorServiceImpl) {
        this.put(OPERATOR_SERVICE_IMPL_CONFIG, operatorServiceImpl);
    }

    /**
     * OPERATOR插件jar名称key
     */
    private final static String OPERATOR_PLUGIN_NAME_CONFIG = "operator.plugin.name";

    public String getOperatorPluginName() {
        return getString(OPERATOR_PLUGIN_NAME_CONFIG);
    }

    public void setOperatorPluginName(T operatorPluginName) {
        this.put(OPERATOR_PLUGIN_NAME_CONFIG, operatorPluginName);
    }

    /**
     * 检查是否满足小粒度汇总条件的频率
     */
    private static final String CALC_PREPROCESSING_INTERVAL_CONFIG = "calc.preprocessing.interval";

    public int getCalcPreprocessInterval() {
        return getInteger(CALC_PREPROCESSING_INTERVAL_CONFIG);
    }

    public void setCalcPreprocessInterval(T calcPreprocesssInterval) {
        this.put(CALC_PREPROCESSING_INTERVAL_CONFIG, calcPreprocesssInterval);
    }

    /**
     * 检查是否满足小粒度汇总条件的频率
     */
    private static final String CALC_GRANULE_INTERVAL_CONFIG = "calc.granule.interval";

    public int getCalcGranuleInterval() {
        return getInteger(CALC_GRANULE_INTERVAL_CONFIG);
    }

    public void setCalcGranuleInterval(T calcGranuleInterval) {
        this.put(CALC_GRANULE_INTERVAL_CONFIG, calcGranuleInterval);
    }

    /**
     * 检查是否满足大粒度汇总条件的频率
     */
    private static final String CALC_BIG_GRANULE_INTERVAL_CONFIG = "calc.big.granule.interval";

    public int getCalcBigGranuleInterval() {
        return getInteger(CALC_BIG_GRANULE_INTERVAL_CONFIG);
    }

    public void setCalcBigGranuleInterval(T calcBigGranuleInterval) {
        this.put(CALC_BIG_GRANULE_INTERVAL_CONFIG, calcBigGranuleInterval);
    }

    /***
     * 粒度汇总结果topic
     */
    private static final String PRODUCER_TOPIC_CONFIG = "producer.topic";

    public String getResultTopic() {
        return getString(PRODUCER_TOPIC_CONFIG);
    }
    public void setResultTopic(T producerDayTopic) {
        this.put(PRODUCER_TOPIC_CONFIG, producerDayTopic);
    }

    /***
     * 粒度汇总结果topic
     */
    private static final String PRODUCER_TOPIC_DAY_CONFIG = "producer.day.topic";

    public String getResultDayTopic() {
        return getString(PRODUCER_TOPIC_DAY_CONFIG);
    }
    public void setResultDayTopic(T producerDayTopic) {
        this.put(PRODUCER_TOPIC_DAY_CONFIG, producerDayTopic);
    }

    /**
     * 数据汇总的粒度集合
     */
    private static final String GRANULE_CONFIG = "granule";

    public String getGranuleMap() {
        return getString(GRANULE_CONFIG);
    }

    public void setGranuleMap(T granuleMap) {
        this.put(GRANULE_CONFIG, granuleMap);
    }

    /**
     * 数据汇总的最后一个粒度
     */
    private static final String TAIL_GRANULE_CONFIG = "tail.granule";

    public int tailGranule() {
        return getInteger(TAIL_GRANULE_CONFIG);
    }

    public void setTailGranule(T tailGranule) {
        this.put(TAIL_GRANULE_CONFIG, tailGranule);
    }

    /**
     * 数据汇总的第一个粒度
     */
    private static final String FIRST_GRANULE_CONFIG = "first.granule";

    public int firstGranule() {
        return getInteger(FIRST_GRANULE_CONFIG);
    }

    public void setFirstGranule(T firstGranule) {
        this.put(FIRST_GRANULE_CONFIG, firstGranule);
    }

    /**
     * 当前拓扑依赖的上一个拓扑
     */
    private static final String LAST_TOPO_CONFIG = "last.topology";

    public String  lastTopology() {
        return getString(LAST_TOPO_CONFIG);
    }

    public void setLastTopology(T lastTopology) {
        this.put(LAST_TOPO_CONFIG, lastTopology);
    }

    /**
     * 当前拓扑依赖的下一个拓扑
     */
    private static final String NEXT_TOPO_CONFIG = "next.topology";

    public String nextTopology() {
        return getString(NEXT_TOPO_CONFIG);
    }

    public void setNextTopology(T nextTopology) {
        this.put(NEXT_TOPO_CONFIG, nextTopology);
    }

    /**
     * 数据延迟计算时间,单位:秒
     */
    private static final String DELAY_CALC_TIME_CONFIG = "delay.calc.time";

    public int getDelayCalcTime() {
        return getInteger(DELAY_CALC_TIME_CONFIG);
    }

    public void setDelayCalcTime(T delayCalcTime) {
        this.put(DELAY_CALC_TIME_CONFIG, delayCalcTime);
    }

    /**
     * 基线拓扑数据保留时长,单位:秒
     */
    private static final String BASELINE_EXPIRE_CONFIG = "baseline.expire";

    public int getBaselineExpire() {
        return getInteger(BASELINE_EXPIRE_CONFIG);
    }

    public void setBaselineExpire(T baselineExpire) {
        this.put(BASELINE_EXPIRE_CONFIG, baselineExpire);
    }

    /**
     * 报警数据计算周期,单位:天
     */
    private static final String BASELINE_CYCLE_CONFIG = "baseline.cycle";

    public int getBaselineCycle() {
        return getInteger(BASELINE_CYCLE_CONFIG);
    }

    public void setBaselineCycle(T baselineCycle) {
        this.put(BASELINE_CYCLE_CONFIG, baselineCycle);
    }

    /**
     * 检查是否满足基线汇总条件的频率
     */
    private static final String CALC_BASELINE_INTERVAL_CONFIG = "calc.baseline.interval";

    public int getCalcBaselineInterval() {
        return getInteger(CALC_BASELINE_INTERVAL_CONFIG);
    }

    public void setCalcBaselineInterval(T baselineInterval) {
        this.put(CALC_BASELINE_INTERVAL_CONFIG, baselineInterval);
    }

    /**
     * 检查是否满足报警汇总条件的频率
     */
    private static final String CALC_ALERT_INTERVAL_CONFIG = "calc.alert.interval";

    public int getCalcAlertInterval() {
        return getInteger(CALC_ALERT_INTERVAL_CONFIG);
    }

    public void setCalcAlertInterval(T alertInterval) {
        this.put(CALC_ALERT_INTERVAL_CONFIG, alertInterval);
    }

    /**
     * 自定义存储插件topic名称
     */
    private static final String CUSTOM_STORAGE_TOPIC_CONFIG = "custom.storage.topic";

    public String getCustomStorageTopic() {
        return getString(CUSTOM_STORAGE_TOPIC_CONFIG);
    }
    public void setCustomStorageTopic(T producerDayTopic) {
        this.put(CUSTOM_STORAGE_TOPIC_CONFIG, producerDayTopic);
    }

    /**
     * 概述：Kafka服务地址
     */
    public String getBootstrapServers() {
        return getString(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
    }

    public void setBootstrapServers(T bootstrapServers) {
        this.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    public String getGroupId() {
        return getString(ConsumerConfig.GROUP_ID_CONFIG);
    }

    public void setGroupId(T groupId) {
        this.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    }

    public int getMsgMaxByte() {
        return getInteger(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG);
    }

    public void setMsgMaxByte(T maxByte) {
        this.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxByte);
    }

    public String getMaxPollCount() {
        return getString(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
    }

    public void setMaxPollCount(T maxPollCount) {
        this.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollCount);
    }

    // 数据库相关
    private static final String DB_PARAMS_CONFIG = "db_config";

    public String getDbParams() {
        return getString(DB_PARAMS_CONFIG);
    }

    public void setDbParams(T dbParams) {
        this.put(DB_PARAMS_CONFIG, dbParams);
    }

    public static final String DB_USER = "db_user";
    public static final String DB_PSWD = "db_pswd";
    public static final String DB_DRIVER = "db_driver";
    public static final String DB_URL = "db_url";
    public static final String DB_CHECKOUT_TIME = "checkout.timeout";
    public static final String DB_MAX_STATEMENTS_PER_CONNECTION = "max.statements.per.connection";
    public static final String DB_INITIAL_POOL_SIZE = "initial.pool.size";
    public static final String DB_MAX_IDLE_TIME = "max.idle.time";
    public static final String DB_MAX_POOL_SIZE = "max.pool.size";
    public static final String DB_MIN_POOL_SIZE = "min.pool.size";
    public static final String DB_ACQUIRE_INCREMENT = "acquire.increment";
    public static final String DB_IDLE_CONNECTION_TEST_PERIOD = "idle.connection.test.period";
    public static final String DB_NUM_HELPER_THREADS = "num.helper.threads";
    public static final String DB_PREFERRED_TEST_QUERY = "preferred.test.query";
    public static final String DB_STATEMENT_CACHE_NUM_DEFERRED_CLOSE_THREADS = "statement.cache.num.deferred.close.threads";
    public static final String DB_UNRETURNED_CONNECTION_TIMEOUT = "unreturned.connection.timeout";

    public String getString(String key) {
        if (this.containsKey(key)) {
            Object value = this.get(key);
            return value == null ? "" : value.toString();
        }
        return null;
    }

    public boolean getBoolean(String key) {
        if (this.containsKey(key)) {
            Object value = this.get(key);
            return Boolean.TRUE.equals(value);
        }
        return false;
    }

    public int getInteger(String key) {
        String result = getString(key);
        return result != null ? Integer.parseInt(result) : 0;
    }

    public double getDouble(String key) {
        String result = getString(key);
        return result != null ? Double.parseDouble(result) : 0D;
    }

    public long getLong(String key) {
        String result = getString(key);
        return result != null ? Long.parseLong(result) : 0L;
    }

    public T getNumber(String key) {
        Object result = this.get(key);
        return result != null ? (T) result : null;
    }
}
