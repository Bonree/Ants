package com.bonree.ants.commons.granule.topology;

import bonree.proxy.JedisProxy;
import com.alibaba.fastjson.JSON;
import com.bonree.ants.commons.*;
import com.bonree.ants.commons.enums.BizType;
import com.bonree.ants.commons.enums.Delim;
import com.bonree.ants.commons.granule.GranuleCommons;
import com.bonree.ants.commons.granule.GranuleFinal;
import com.bonree.ants.commons.granule.model.GranuleMap;
import com.bonree.ants.commons.redis.JedisClient;
import com.bonree.ants.commons.redis.RedisGlobals;
import com.bonree.ants.commons.schema.SchemaGLobals;
import com.bonree.ants.commons.schema.model.SchemaData;
import com.bonree.ants.commons.utils.DateFormatUtils;
import com.bonree.ants.commons.zookeeper.NodeListener;
import com.bonree.ants.commons.zookeeper.ZKClient;
import com.bonree.ants.commons.zookeeper.ZKGlobals;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.*;
import java.util.Map.Entry;

/**
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018/8/30 14:09
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 粒度汇总控制类
 * *****************************************************************************
 */
public abstract class GranuleControllerSpout extends GranuleSpout implements IRichSpout {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(GranuleControllerSpout.class);

    private volatile long startTime = 0;    // 用于控制汇总频率
    private volatile long diffTime = 0;     // 触发汇总的频率
    private NodeListener redisNode;         // redis状态监听
    private NodeListener kafkaNode;         // kafka状态监听
    private String allGranuleKey;           // 用于获取拓扑所有粒度的key
    protected String currGranuleKey;        // 用于获取当前正在计算的业务的key
    private JedisProxy redis;               //redis连接对象
    protected GranuleMap graMap;              // 当前拓扑计算的粒度集合

    /**
     * 拓扑名称
     */
    protected String topologyName;

    /**
     * 检查粒度汇总的频率
     */
    protected int checkAggInterval = 1000;

    /**
     * SPOUT发射器.
     */
    protected SpoutOutputCollector collector;

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        // 1.初始配置文件
        Commons.initAntsSchema(conf);
        graMap = JSON.parseObject(GlobalContext.getConfig().getGranuleMap(), GranuleMap.class);
        initConf(conf);

        // 2.初始参数
        currGranuleKey = String.format(GranuleFinal.CURR_GRANULE_KEY, topologyName);
        allGranuleKey = String.format(GranuleFinal.ALL_GRANULE_KEY, topologyName);

        // 3.初始化redis对象
        redis = JedisClient.init();

        // 4.重置当前拓扑所需要计算的粒度集合
        checkGranuleList(redis, true);

        // 5.检测每个粒度下所有业务类型的分桶计算是否完整
        checkAggBizBucketCount();

        ZKClient client = new ZKClient(GlobalContext.getConfig().getZookeeperCluster());
        // 6.监听redis状态
        redisNode = new NodeListener(ZKGlobals.REDIS_MONITOR_PATH, client.getCuratorFramework());
        redisNode.addNodeListener();

        // 7.监听kafka状态
        kafkaNode = new NodeListener(ZKGlobals.KAFKA_MONITOR_PATH, client.getCuratorFramework());
        kafkaNode.addNodeListener();
    }

    @Override
    public void nextTuple() {
        try {
            // 1.控制访问频率
            if (accessInterval(checkAggInterval)) {
                return;
            }
            // 2.依赖环境验证
            if (baseValidate()) {
                return;
            }
            if (redis == null) {
                redis = JedisClient.init();
            }
            emitData(redis);
        } catch (Exception ex) {
            log.error("AggSpout nextTuple error", ex);
        } finally {
            JedisClient.close(redis);
        }
    }

    @Override
    public void ack(Object msgId) {
        if (msgId == null) {
            return;
        }
        ackMsg((Values) msgId);
    }

    @Override
    public void fail(Object msgId) {
        log.info("Ack fail, msg:{}", msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("granule", "bizType", "bizName", "timeList", "bucketNum"));
    }

    @Override
    public void close() {
        log.info("{} worker close...", topologyName);
        if (redisNode != null) {
            redisNode.close();
        }
        if (kafkaNode != null) {
            kafkaNode.close();
        }
    }

    @Override
    public void activate() {
        log.info("{} worker activate...", topologyName);
    }

    @Override
    public void deactivate() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    /**
     * 概述：进入频率控制
     */
    private boolean accessInterval(int aggInterval) {
        long currTime = System.currentTimeMillis();
        diffTime = currTime - startTime;
        if (diffTime < aggInterval) {
            return true; // 1.访问频率小于指定间隔时跳出
        }
        diffTime = (startTime == 0) ? aggInterval : diffTime;
        log.info("Come {} spout, diffTime: {}, interval: {}", topologyName, diffTime, aggInterval);
        startTime = currTime;
        return false;
    }

    /**
     * 概述：依赖环境验证
     */
    private boolean baseValidate() {
        if (redisNode != null && Boolean.valueOf(redisNode.getData())) {
            log.warn("Redis cluster exception! please wait....");
            return true; // 2.redis集群不正常时跳出
        }
        if (kafkaNode != null && Boolean.valueOf(kafkaNode.getData())) {
            log.warn("Kafka cluster exception! please wait....");
            return true; // 3.kafka集群不正常时跳出
        }
        return false;
    }

    /**
     * 概述：发射数据,如有需要子类可重写
     *
     * @param redis redis对象
     */
    protected void emitData(JedisProxy redis) throws Exception {
        // 1.检查并修正时间粒度.
        checkGranuleList(redis, false);
        // 2.从REDIS中获取需要汇总的时间粒度
        boolean isCalcing = false; // true 表示当前粒度正在计算,false 表示当前粒度未开始计算
        String tmpCurrGra = redis.get(currGranuleKey); // 获取当前正在汇总的粒度
        if (StringUtils.isEmpty(tmpCurrGra)) { // tmpCurrGra为空,表示当前粒度时各类型维度的数据已经计算完成,需要计算下一个粒度数据.
            tmpCurrGra = redis.lpop(allGranuleKey); // 从队列的最上面取出需要汇总的粒度
            redis.rpush(allGranuleKey, tmpCurrGra); // 把取出的粒度再放回队列的最后面再进行排队,等待下次汇总.
        } else {
            isCalcing = true;
        }
        emitPrepare(redis, tmpCurrGra);
        // 3.发射条件验证
        if (emitValidate(redis, Integer.parseInt(tmpCurrGra))) {
            return;
        }
        // 4.累计超时时间
        increaseTimeOut(redis, diffTime);
        // 5.检查符合计算的粒度发给后面的bolt进行计算
        if (!emitController(Integer.parseInt(tmpCurrGra), isCalcing, redis)) {
            redis.del(currGranuleKey); // 未计算,清除当前标记的粒度
        }
    }

    /**
     * 概述：粒度发射控制器
     *
     * @param granule   当前时间粒度
     * @param isCalcing 当前粒度是否在计算中
     * @param redis     redis实例
     * @return true 发射; false 未发射.
     */
    private boolean emitController(int granule, boolean isCalcing, JedisProxy redis) {
        log.info("Start granule agg....{}, isCalcing:{}", granule, isCalcing);
        try {
            String graKey = String.format(GranuleFinal.AGG_TIME_KEY, bizType(), granule); // 当前粒度KEY
            List<Entry<String, List<String>>> sortList = sortMap(redis.hgetAll(graKey)); // 获取计算当前粒度数据时所需要的所有数据的查询条件<数据时间戳,汇总该时间所需要的时间集合> 并排序及整理
            sortList = deleteExpireKey(redis, sortList, granule, graKey);      // 删除已经计算过的粒度时间key
            initGranuleTimeout(redis, granule, sortList);                      // 初始当前粒度每个时间戳的超时时间
            if (sortList.isEmpty()) {
                return false;
            }
            String granuleTime = sortList.get(0).getKey();      // 每次只汇总排序后最前面的时间戳数据
            long time = DateFormatUtils.parse(granuleTime, GlobalContext.SEC_PATTERN).getTime();
            if (!isCalcing && delayCalc(granule, time)) {
                return false;
            }
            int lastGra = graMap.lastGra(granule);
            List<String> upGraList = sortList.get(0).getValue();// 汇总当前数据所需要的时间戳(上一粒度)
            boolean lengthCalc = upGraList.size() == (granule / lastGra); // 是否满足计算所需要的长度要求
            boolean timeoutCalc = !lengthCalc && getTimeout(redis, granule, granuleTime); // 是否满足计算的超时时间
            // 符合这三种情况将进行粒度汇总.1.正在计算中的粒度; 2.符合汇总时所需要的条数; 3.达到超时时间;
            if (!isCalcing && !lengthCalc && !timeoutCalc) {
                return false;
            }
            if (!isCalcing) { // 达到汇总条件后,表示该粒度第一次处理
                if (intervalLimit(redis, granule, granuleTime)) { // 检测粒度计算的频度.
                    return false;
                }
                log.info("Granule:{}, time:{}, lengthCalc:{}, timeoutCalc:{}, timeList:{}", granule, granuleTime, lengthCalc, timeoutCalc, upGraList);
                redis.setex(currGranuleKey, GranuleFinal.CURRENT_CALC_EXPIRE, granule + ""); // 记录开始计算的粒度
            }
            Map<String, SchemaData> bizMap = SchemaGLobals.getSchemaByType(time, bizType()); // 获取当前业务需要计算的业务名称集合
            if (bizMap == null || bizMap.isEmpty()) {
                log.warn("Get bizNameSet is empty !!! time: {}, type: {}", time, bizType());
                return false;
            }
            customCalcJob(granule, time); // 自定义业务计算
            Map<String, Map<Integer, Boolean>> bucketMap = getBizBucketMap(redis, bizMap.keySet(), granule, granuleTime);
            for (Entry<String, Map<Integer, Boolean>> entry : bucketMap.entrySet()) { // 每次只会处理一个业务
                String bizName = entry.getKey(); // 业务名称
                for (Entry<Integer, Boolean> bucket : entry.getValue().entrySet()) {
                    long emitTime = System.currentTimeMillis();
                    int bucketNumber = bucket.getKey();
                    boolean isTailBucket = bucket.getValue();
                    collector.emit(new Values(granule, bizType(), bizName, upGraList, bucketNumber - 1), new Values(graKey, granuleTime, bizName, isTailBucket, granule, bucketNumber - 1, emitTime));
                    log.info("Emit data to bolt, type: {}, granule: {}, time: {}, bucket: {}", bizName, granule, granuleTime, bucketNumber);
                }
            }
            if (granule == GlobalContext.getConfig().firstGranule()) { // 设置计算开始标记
                String calcStatusKey = String.format(GranuleFinal.GRANULE_CALC_STATUS_KEY, bizType(), granuleTime);
                if (redis.setnx(calcStatusKey, RedisGlobals.CALCED_FLAG) == 1) {
                    redis.expire(calcStatusKey, GranuleFinal.CURRENT_CALC_EXPIRE);
                }
            }
            return true;
        } catch (Exception e) {
            log.error("EmitController error !", e);
        }
        return false;
    }

    /**
     * 概述：获取业务集合需要计算数据时所需要的分桶编号集合
     *
     * @param redis       redis对象
     * @param bizSets     业务集合
     * @param granule     当前粒度
     * @param granuleTime 粒度时间
     * @return <业务名称,<分桶编号,是否为最后一个分桶>>
     */
    protected Map<String, Map<Integer, Boolean>> getBizBucketMap(JedisProxy redis, Set<String> bizSets, int granule, String granuleTime) {
        int i = 0;            // 记录计算业务的个数.
        int bizSize = bizSets.size();
        Map<Integer, Boolean> bucketMap;
        Map<String, Map<Integer, Boolean>> bizBucketMap = new HashMap<>();
        int redisBucket = graMap.get(granule).getBucket();
        int calcParallel = graMap.get(granule).getParallel();
        // 记录计算到当前粒度的所有业务分桶数(hset):key:normal#60#20180426141800#curr_agg_biz,filed:业务类型,value:分桶数
        String currBizKey = String.format(GranuleFinal.CURR_AGG_BIZ_KEY, bizType(), granule, granuleTime);
        for (String bizName : bizSets) {// 遍历每种业务类型
            i++;
            int bucketCount = 0;  // 记录当前业务已计算过的分桶数量.
            if (!redis.hexists(currBizKey, bizName)) {
                redis.hset(currBizKey, bizName, "0");
                redis.expire(currBizKey, GranuleFinal.CURRENT_CALC_EXPIRE);
            } else {
                bucketCount = Integer.parseInt(redis.hget(currBizKey, bizName));
                if (bucketCount >= redisBucket) {
                    continue; // 表示当前类型的分桶数据已经计算完成
                }
            }
            if (!bizBucketMap.containsKey(bizName)) {
                bizBucketMap.put(bizName, new HashMap<Integer, Boolean>());
            }
            bucketMap = bizBucketMap.get(bizName);
            for (int n = 0; bucketCount < redisBucket; n++) {// 遍历每个类型的分桶数量
                if (bucketCount > 0 && n > 0 && n % calcParallel == 0) {
                    break; // 每次只汇总指定数量的分桶.
                }
                redis.hset(currBizKey, bizName, (++bucketCount) + "");
                boolean isLastBiz = i == bizSize && bucketCount == redisBucket;// true 表示当前粒度下所有业务的所有分桶计算完成, 否则为false
                bucketMap.put(bucketCount, isLastBiz);
            }
            break; // 每次只处理一个类型
        }
        if (bizBucketMap.isEmpty()) {
            log.info("Load bizBucketMap is empty! key: {}, redisBucket: {}", currBizKey, redisBucket);
        }
        return bizBucketMap;
    }

    /**
     * 概述：删除过期粒度汇总的时间key
     *
     * @param redis    redis对象,切记调用方法时,记得关闭连接.
     * @param sortList 排序后的时间集合
     * @param currGra  当前粒度
     * @param graKey   当前粒度KEY
     */
    private List<Entry<String, List<String>>> deleteExpireKey(JedisProxy redis, List<Entry<String, List<String>>> sortList, int currGra, String graKey) {
        List<Entry<String, List<String>>> newList = new ArrayList<>();
        for (Entry<String, List<String>> entry : sortList) {    // 1.遍历处理每个需要计算的粒度; 在同一汇总粒度下, 可能存在两个时间KEY
            String granuleTime = entry.getKey();// 汇总当前数据的时间戳
            String calcedFlag = !entry.getValue().isEmpty() && entry.getValue().size() == 1 ? entry.getValue().get(0) : "";
            boolean isGranuled = calcedFlag.startsWith(RedisGlobals.CALCED_FLAG);
            try {
                if (isGranuled) { // 已经汇总过的数据,不再进行汇总
                    String granuleTimeOutKey = String.format(GranuleFinal.TIMEOUT_KEY, topologyName, currGra);
                    if (redis.hexists(granuleTimeOutKey, granuleTime)) {
                        redis.hdel(granuleTimeOutKey, granuleTime); // 删除已经计算过的控制超时计算的粒度时间
                    }
                    String[] tmpTime = calcedFlag.split(Delim.SIGN.value()); // 分割粒度计算结果,格式:"-1#long类型时间戳"s
                    Date calcedTime = tmpTime.length == 2 ? new Date(Long.parseLong(tmpTime[1])) : DateFormatUtils.parse(granuleTime, GlobalContext.SEC_PATTERN);
                    Date date = DateFormatUtils.addSeconds(calcedTime, currGra * 5);
                    if (System.currentTimeMillis() > date.getTime()) {
                        redis.hdel(graKey, granuleTime); // 删除已经处理过的数据KEY
                        log.info("Delete expire granule key:{}, time:{}, currGra:{}", graKey, granuleTime, currGra);
                    }
                    continue;
                }
                newList.add(entry);
            } catch (Exception ex) {
                log.error("Process expire granuleTime error! ", ex);
            }
        }
        return newList;
    }

    /**
     * 概述：对开始粒度汇总的标识进行排序
     *
     * @param aggKeyMap 粒度时间串集合
     */
    private static List<Entry<String, List<String>>> sortMap(Map<String, String> aggKeyMap) {
        Map<String, List<String>> granuleMap = new HashMap<>();
        String[] timeArr;
        for (Entry<String, String> entry : aggKeyMap.entrySet()) { // 整理集合
            String granuleTime = entry.getKey();
            String value = entry.getValue();
            if (StringUtils.isEmpty(value) || StringUtils.isEmpty(granuleTime)) {
                continue;
            }
            timeArr = value.split(Delim.COMMA.value());// 分割时间戳字符串
            if (timeArr.length == 0) {
                continue;
            }
            granuleMap.put(granuleTime, Arrays.asList(timeArr));
        }
        List<Entry<String, List<String>>> sortList = new ArrayList<>(granuleMap.entrySet());
        Collections.sort(sortList, new Comparator<Entry<String, List<String>>>() {
            @Override
            public int compare(Entry<String, List<String>> o1, Entry<String, List<String>> o2) {
                String start1 = o1.getKey();
                String start2 = o2.getKey();
                return start1.compareTo(start2); // 按key进行排序
            }
        });
        return sortList;
    }

    /**
     * 概述：检查粒度集合是否正确
     *
     * @param redis   redis对象,切记调用方法时,记得关闭连接.
     * @param isReset 是否强制重置粒度集合. true: 强制, false:不强制
     */
    private void checkGranuleList(JedisProxy redis, boolean isReset) {
        long length = graMap.size();
        long aggLength = redis.llen(allGranuleKey);// REDIS中存储的汇总粒度的个数
        if (length != aggLength || isReset) {
            redis.del(allGranuleKey);
            for (int granule : graMap.keySet()) {
                redis.rpush(allGranuleKey, granule + "");
            }
            log.warn("Reset granule {}: {} ...", topologyName, graMap.keySet());
        }
    }

    /**
     * 概述：初始需要粒度汇总的时间key的超时时间
     *
     * @param redis    redis对象,切记调用方法时,记得关闭连接.
     * @param granule  数据粒度
     * @param sortList 排序后的粒度时间串集合
     */
    private void initGranuleTimeout(JedisProxy redis, int granule, List<Entry<String, List<String>>> sortList) {
        String total = "0";
        List<String> upGraList;
        String granuleTimeOutKey = String.format(GranuleFinal.TIMEOUT_KEY, topologyName, granule); // 汇总粒度等待时间存在REDIS中的key
        for (Entry<String, List<String>> entry : sortList) {// 1.遍历处理每个需要计算的粒度; 在同一汇总粒度下, 可能存在两个时间KEY
            String granuleTime = entry.getKey();            // 汇总当前数据的时间戳
            upGraList = entry.getValue();                   // 汇总当前数据所需要的时间戳(上一粒度)
            if (redis.hexists(granuleTimeOutKey, granuleTime)) {
                continue;
            }
            if (!upGraList.isEmpty()) {
                total = GranuleCommons.correctTimeout(granule, graMap.lastGra(granule), upGraList.get(upGraList.size() - 1));
                log.info("Init granule {} timeout: {}, granuleTime: {}", granule, total, granuleTime);
            }
            redis.hset(granuleTimeOutKey, granuleTime, total); // 当前粒度默认设置等待时间为0.
        }
    }

    /***
     * 概述： 累加各个粒度的超时时间
     * @param redis redis对象,切记调用方法时,记得关闭连接.
     * @param time 时间间隔.
     */
    private void increaseTimeOut(JedisProxy redis, long time) {
        Map<String, String> timeOutMap;
        String granuleTime;
        for (int granule : graMap.keySet()) {
            String timeoutKey = String.format(GranuleFinal.TIMEOUT_KEY, topologyName, granule);
            if (!redis.exists(timeoutKey)) {
                continue;
            }
            timeOutMap = redis.hgetAll(timeoutKey);
            for (Map.Entry<String, String> entry : timeOutMap.entrySet()) {
                granuleTime = entry.getKey();
                long timeout = StringUtils.isEmpty(entry.getValue()) ? 0 : Integer.parseInt(entry.getValue());
                if (timeout >= GranuleFinal.DATA_EXPIRE * 1000) { // 超过两天的过期时间将删除
                    try {
                        redis.hdel(timeoutKey, granuleTime);
                        log.info("Timeout delete bigger than 48h key {} granuleTime {}", timeoutKey, granuleTime);
                    } catch (Exception e) {
                        log.error("Timeout delete key :{} granuleTime:{} error :{}", timeoutKey, granuleTime, e);
                    }
                    continue;
                }
                redis.hset(timeoutKey, granuleTime, String.valueOf(timeout + time));
            }
        }
    }

    /**
     * 概述: 获取各粒度的超时时间
     *
     * @param redis       redis对象,切记调用方法时,记得关闭连接.
     * @param granule     数据汇总粒度
     * @param granuleTime 数据时间
     */
    private boolean getTimeout(JedisProxy redis, int granule, String granuleTime) {
        String granuleTimeOutKey = String.format(GranuleFinal.TIMEOUT_KEY, topologyName, granule); // 汇总粒度等待时间存在REDIS中的key
        String total = redis.hget(granuleTimeOutKey, granuleTime);
        long curTotal = StringUtils.isEmpty(total) ? 0 : Long.parseLong(total);
        if (graMap == null || graMap.isEmpty()) {
            return false;
        }
        int timeout = graMap.get(granule).getTimeout();
        GlobalContext.LOG.debug("GetTimeout:{}, time:{},{}>{}+{}", granuleTimeOutKey, granuleTime, curTotal, timeout, granule * 1000);
        return curTotal > (timeout + granule * 1000);
    }

    /**
     * 概述：处理ack信息,如有需要子类可重写
     *
     * @param ackValue ack信息对象
     */
    protected void ackMsg(Values ackValue) {
        if (ackValue.size() != 7) {
            return;
        }
        String graKey = ackValue.get(0).toString();
        String currGraTime = ackValue.get(1).toString();
        String bizName = ackValue.get(2).toString();
        boolean isTailBiz = Boolean.valueOf(ackValue.get(3).toString());
        String curGra = ackValue.get(4).toString();
        String bucket = ackValue.get(5).toString();
        long emitTime = Long.parseLong(ackValue.get(6).toString());
        if (isTailBiz) { // 一个粒度的所有类型计算完成后,再打标记.
            try {
                if (redis == null) {
                    redis = JedisClient.init();
                }

                // 1.对已经汇总过的数据KEY打上标记
                markingCalcStatus(redis, graKey, currGraTime);// 标记计算完成
                // 2.计算完成,清除当前已经计算过的维度类型.
                String currAggBizKey = String.format(GranuleFinal.CURR_AGG_BIZ_KEY, bizType(), curGra, currGraTime);
                redis.del(currAggBizKey);
                // 3. 计算完成,清除当前已经计算过的粒度
                redis.del(currGranuleKey);
                // 4. 计算完成,删除当前粒度的等待时间
                String granuleTimeOutKey = String.format(GranuleFinal.TIMEOUT_KEY, topologyName, curGra);
                redis.hdel(granuleTimeOutKey, currGraTime);
                int granule = Integer.parseInt(curGra);
                // 5.保存粒度汇总标识
                int tailGranule = GlobalContext.getConfig().tailGranule();
                if (granule != tailGranule) { // 跳过不保存redis的中间结果,一般为最后一个粒度
                    GranuleCommons.granuleController(redis, bizType(), graMap.nextGra(granule), currGraTime);
                }
                // 6.更新redis状态
                uploadRedisStatus(redis, currGraTime, granule);
            } catch (Exception ex) {
                log.error("UploadRedisStatus error!", ex);
            } finally {
                JedisClient.close(redis);
            }
        }
        long end = System.currentTimeMillis();
        log.info("Ack success, msg: {}|{}|{}|{}|{}, costing: {}", graKey, currGraTime, bizName, isTailBiz, bucket, (end - emitTime));
    }

    /**
     * 概述：标记粒度时间的计算状态
     *
     * @param redis   redis对象
     * @param graKey  粒度key
     * @param graTime 粒度时间
     */
    private void markingCalcStatus(JedisProxy redis, String graKey, String graTime) {
        Map<String, String> map = new HashMap<>();
        map.put(graTime, String.format(GranuleFinal.CALCED_TIME_VALUE, RedisGlobals.CALCED_FLAG, System.currentTimeMillis()));
        redis.hmset(graKey, map);
    }

    /**
     * 概述：检测每个粒度下所有业务类型的分桶计算是否完整
     */
    private void checkAggBizBucketCount() {
        if (bizType().equals(BizType.ALERT.type())) {
            return; // 报警业务不需要检查
        }
        JedisProxy redis = null;
        Set<String> bizNameSet;
        List<String> calcedList;
        try {
            redis = JedisClient.init();
            for (int granule : graMap.keySet()) { // 粒度
                int redisBucket = graMap.get(granule).getBucket();
                String upGraKey = String.format(GranuleFinal.AGG_TIME_KEY, bizType(), granule);
                Map<String, String> aggKeyMap = redis.hgetAll(upGraKey);
                if (aggKeyMap == null || aggKeyMap.isEmpty()) {
                    continue;
                }
                for (Entry<String, String> entry : aggKeyMap.entrySet()) { // 粒度下的每个数据时间戳
                    String granuleTime = entry.getKey();
                    long time = DateFormatUtils.parse(granuleTime, GlobalContext.SEC_PATTERN).getTime();
                    bizNameSet = SchemaGLobals.getSchemaByType(time, bizType()).keySet();
                    if (bizNameSet.isEmpty()) {
                        continue;
                    }
                    calcedList = new ArrayList<>();
                    String currBizKey = String.format(GranuleFinal.CURR_AGG_BIZ_KEY, bizType(), granule, granuleTime);
                    for (String bizName : bizNameSet) {// 每个数据时间戳下的每种业务类型
                        if (!redis.hexists(currBizKey, bizName)) {
                            continue;
                        }
                        int bucketCount = Integer.parseInt(redis.hget(currBizKey, bizName));
                        if (bucketCount >= redisBucket) {
                            calcedList.add(bizName);
                        }
                    }
                    if (!calcedList.isEmpty()) {
                        markingCalcStatus(redis, upGraKey, granuleTime);
                        redis.del(currBizKey);      // 清除当前已经计算过的维度类型.
                        log.info("Delete calced garnule key: {}", currBizKey);
                    }
                }
            }
        } catch (ParseException ex) {
            log.error("CheckAggBizBucketCount error !", ex);
        } finally {
            JedisClient.close(redis);
        }
    }
}
