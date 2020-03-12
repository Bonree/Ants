package com.bonree.ants.calc.topology.preprocess;

import bonree.proxy.JedisProxy;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bonree.ants.calc.quartz.StartQuartz;
import com.bonree.ants.commons.Commons;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.granule.GranuleCommons;
import com.bonree.ants.commons.kafka.ConsumerClient;
import com.bonree.ants.commons.kafka.KafkaGlobals;
import com.bonree.ants.commons.redis.JedisClient;
import com.bonree.ants.commons.redis.RedisGlobals;
import com.bonree.ants.commons.utils.DateFormatUtils;
import com.bonree.ants.commons.zookeeper.NodeListener;
import com.bonree.ants.commons.zookeeper.ZKClient;
import com.bonree.ants.commons.zookeeper.ZKGlobals;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.bonree.ants.commons.GlobalContext.PREPROCESSING;
import static com.bonree.ants.commons.GlobalContext.SEC_PATTERN;
import static com.bonree.ants.commons.granule.GranuleFinal.CALC_POSITION_KEY;
import static com.bonree.ants.commons.redis.RedisGlobals.CALC_POSITION_EXPIRE;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月10日 下午12:09:17
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 数据入口-处理kafka数据
 * ****************************************************************************
 */
public class KafkaSpout implements IBatchSpout {

    private static final long serialVersionUID = 1L;

    private static final Logger log = LoggerFactory.getLogger(KafkaSpout.class);

    private NodeListener redisNode; // redis状态监听
    private NodeListener kafkaNode; // kafka状态监听
    private long lastEmitTime;      // 上次消费数据的时间
    private String taskId;          // 当前spout的任务id
    private JedisProxy jedis;       // redis连接对象
    private final List<String> topic;     // 消息主题
    private final int batchSize;    // 数据包批量处理大小

    public KafkaSpout(List<String> topic, int batchSize) {
        this.topic = topic;
        this.batchSize = batchSize;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context) {
        this.taskId = String.valueOf(context.getThisTaskId());
        // 1.初始配置参数
        Commons.initAntsSchema(conf);

        // 2.启动检测服务
        jedis = JedisClient.init();
        if (JedisClient.syncLock(jedis, RedisGlobals.CHECK_CALC_JOB_KEY, 6)) {
            StartQuartz.run(); // 定时检查任务
        }

        // 3.初始消息拉取实例
        ConsumerClient.instance.consumerRecord(topic, GlobalContext.getConfig());

        ZKClient client = new ZKClient(GlobalContext.getConfig().getZookeeperCluster());
        // 4.监听redis状态
        redisNode = new NodeListener(ZKGlobals.REDIS_MONITOR_PATH, client.getCuratorFramework());
        redisNode.addNodeListener();

        // 5.监听kafka状态
        kafkaNode = new NodeListener(ZKGlobals.KAFKA_MONITOR_PATH, client.getCuratorFramework());
        kafkaNode.addNodeListener();

        // 6.重置标记
        JedisClient.deleteRedisByKey(KafkaGlobals.SDK_CONSUMER_TIME_KEY);
        String positionKey = String.format(CALC_POSITION_KEY, PREPROCESSING);
        JedisClient.setByKey(positionKey, DateFormatUtils.format(new Date(), SEC_PATTERN), CALC_POSITION_EXPIRE);
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        try {
            if (redisNode != null && Boolean.valueOf(redisNode.getData())) {
                log.warn("Redis cluster exception! please wait.... {}", batchId);
                return; // redis集群不正常时跳出
            }
            if (jedis == null) {
                jedis = JedisClient.init();
            }
            GranuleCommons.setCalcPosition(jedis, PREPROCESSING, true);
            if (kafkaNode != null && Boolean.valueOf(kafkaNode.getData())) {
                log.warn("Kafka cluster exception! please wait.... {}", batchId);
                return; // kafka集群不正常时跳出
            }
            if (GranuleCommons.emitController(jedis, lastEmitTime, GlobalContext.getConfig().nextTopology(), CALC_POSITION_EXPIRE * 2)) {
                return; // granule停止或计算过慢时跳出
            }
            String minRecordTime = "";
            String maxRecordTime = "";
            List<String> sortedConsumerList = getSortedConsumerList(jedis);
            if (sortedConsumerList != null) {
                minRecordTime = sortedConsumerList.get(0);
                maxRecordTime = sortedConsumerList.get(sortedConsumerList.size() - 1);
            }

            if (isConsumerTooFast(minRecordTime)) {
                return;// 当一个分区消费过快时跳出
            }
            List<byte[]> records = ConsumerClient.instance.pullRecord();// 获取一个records
            if (records == null || records.isEmpty()) {
                if (StringUtils.isNotEmpty(maxRecordTime)) { // 将时间设置为最大值，防止这个分区已经消费完了阻塞其他的分区
                    jedis.hset(KafkaGlobals.SDK_CONSUMER_TIME_KEY, taskId, maxRecordTime);
                }
                log.info("Load kafka queue is empty... batchId: {}", batchId);
                return; // 未拉取到数据时跳出
            }
            lastEmitTime = getMonitorTime(records);
            batchSendData(collector, records, batchId);
        } catch (Exception ex) {
            log.error("JmsSpout error !", ex);
        } finally {
            JedisClient.close(jedis);
        }
    }

    @Override
    public void ack(long batchId) {
        if (redisNode != null && Boolean.valueOf(redisNode.getData())) {
            return; // redis集群不正常时跳出
        }
        JedisProxy redis = null;
        try {
            redis = JedisClient.init();
            int preprocessInterval = GlobalContext.getConfig().getCalcPreprocessInterval();
            if (JedisClient.lock(redis, RedisGlobals.CHECK_CALC_JOB_KEY, preprocessInterval / 1000)) {
                String positionKey = String.format(CALC_POSITION_KEY, PREPROCESSING);
                Date positionDate = lastEmitTime == 0 ? new Date() : new Date(lastEmitTime);
                redis.set(positionKey, DateFormatUtils.format(positionDate, SEC_PATTERN)); // etl数据位置,用于控制granule是否计算
                redis.expire(positionKey, CALC_POSITION_EXPIRE);
            }
        } catch (Exception ex) {
            log.error("Save etl position error !", ex);
        } finally {
            JedisClient.close(redis);
        }
        log.info("Ack success[batchid: {}]", batchId);
    }

    @Override
    public void close() {
        try {
            if (redisNode != null) {
                redisNode.close();
            }
            if (kafkaNode != null) {
                kafkaNode.close();
            }
            jedis.expire(RedisGlobals.CHECK_CALC_JOB_KEY, 1);
            log.info("KafkaSpout close!!!");
        } catch (Exception ex) {
            log.error("KafkaSpout close", ex);
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("records", "time");
    }

    /**
     * 概述：获取monitorTime
     *
     * @param records 原始数据
     */
    private long getMonitorTime(List<byte[]> records) {
        String monitorTime = "";
        if (records != null && !records.isEmpty()) {
            String tmpRecords = new String(records.get(0), StandardCharsets.UTF_8);
            JSONObject obj = JSON.parseObject(tmpRecords);
            monitorTime = obj.get("monitorTime").toString();
        }
        if (!"".equals(monitorTime)) {
            return Long.parseLong(monitorTime);
        }
        return 0;
    }

    /**
     * 概述：批量发送数据到下一层BOLT
     *
     * @param collector 发送对象
     * @param records   数据集合
     * @param batchId   当前批次id
     */
    private void batchSendData(TridentCollector collector, List<byte[]> records, long batchId) {
        List<byte[]> batchList = new ArrayList<>();
        int size = records.size();
        long length = 0;
        byte[] record;
        for (int i = 0; i < size; i++) {
            record = records.get(i);
            length += record.length;
            batchList.add(record);
            if (i > 0 && i % batchSize == 0) { // 批量发送
                collector.emit(new Values(batchList, lastEmitTime));
                batchList = new ArrayList<>();
            }
        }
        if (!batchList.isEmpty()) { // 发送最后一批
            collector.emit(new Values(batchList, lastEmitTime));
        }

        String currTime = DateFormatUtils.format(new Date(lastEmitTime), SEC_PATTERN);
        log.info("Batch emit data completed !time: {}, count: {}, length:{}, batchId: {}", currTime, size, length, batchId);
    }

    /**
     * 消费kafka分区数据时间
     *
     * @param redis redis对象
     */
    private List<String> getSortedConsumerList(JedisProxy redis) {
        List<String> sortedRecords = null;
        try {
            Map<String, String> map = redis.hgetAll(KafkaGlobals.SDK_CONSUMER_TIME_KEY);
            if (map != null && !map.isEmpty()) {
                sortedRecords = new ArrayList<>(map.values());
                Collections.sort(sortedRecords);
                log.info("Current kafka consumer timelist: {}", sortedRecords);
            }
        } catch (Exception e) {
            log.error("SortedConsumerList error !!!{}", e);
        }
        return sortedRecords;
    }

    /**
     * @param minRecordTime kafka消费最小时间
     */
    private boolean isConsumerTooFast(String minRecordTime) {
        if (lastEmitTime == 0 || StringUtils.isEmpty(minRecordTime)) {
            return false;
        }
        try {
            long time = lastEmitTime / 1000 * 1000;
            long timeDiff = time - DateFormatUtils.getTime(minRecordTime, SEC_PATTERN);
            // 比最小时间大2分钟
            if (timeDiff > 120000) {
                log.info("Consumer kafka  faster 2 min than minRecordTime, currTime {} , minRecordTime {}", lastEmitTime, minRecordTime);
                return true;
            }
        } catch (Exception e) {
            log.error("CheckConsumerTooFast error !!!{}", e);
        }
        return false;
    }

}
