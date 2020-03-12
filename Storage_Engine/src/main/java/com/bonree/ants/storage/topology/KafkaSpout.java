package com.bonree.ants.storage.topology;

import bonree.proxy.JedisProxy;
import com.bonree.ants.commons.Commons;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.kafka.ConsumerClient;
import com.bonree.ants.commons.kafka.KafkaGlobals;
import com.bonree.ants.commons.redis.JedisClient;
import com.bonree.ants.commons.redis.RedisGlobals;
import com.bonree.ants.commons.utils.DateFormatUtils;
import com.bonree.ants.commons.utils.FileUtils;
import com.bonree.ants.commons.zookeeper.NodeListener;
import com.bonree.ants.commons.zookeeper.ZKClient;
import com.bonree.ants.commons.zookeeper.ZKGlobals;
import com.bonree.ants.storage.quartz.StartQuartz;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

public class KafkaSpout extends BaseRichSpout {

    private static final long serialVersionUID = 1L;

    private static final Logger log = LoggerFactory.getLogger(KafkaSpout.class);
    private SpoutOutputCollector collector; // 发射器
    private NodeListener dbNode;            // db状态监听
    private volatile long startTime = 0;    // 用于控制频率
    private Queue<String> errQueue = new LinkedBlockingDeque<>();     // 存放nextTuple出错时的数据的唯一ID
    private Map<String, byte[]> cacheMap = new ConcurrentHashMap<>(); // 缓存发送给bolt处理的数据,ack成功时清除,ack失败时重发.<KEY:唯一ID,VALUE:数据内容>

    private List<String> topics;

    KafkaSpout(List<String> topics) {
        this.topics = topics;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        // 1.初始配置参数
        Commons.initAntsSchema(conf);

        // 2.定时检查任务
        JedisProxy jedis = null;
        try {
            jedis = JedisClient.init();
            if (JedisClient.syncLock(jedis, RedisGlobals.CHECK_STORAGE_JOB_KEY, 6)) {
                StartQuartz.run();
            }
        } finally {
            JedisClient.close(jedis);
        }

        // 2.初始消息拉取实例
        ConsumerClient.instance.consumerRecord(topics, GlobalContext.getConfig());

        // 3.监听db状态
        if (!GlobalContext.getConfig().useStoragePlugin()) {
            ZKClient client = new ZKClient(GlobalContext.getConfig().getZookeeperCluster());
            dbNode = new NodeListener(ZKGlobals.DB_MONITOR_PATH, client.getCuratorFramework());
            dbNode.addNodeListener();
        }
    }

    @Override
    public void nextTuple() {
        if (dbNode != null && Boolean.valueOf(dbNode.getData())) {
            long currTime = System.currentTimeMillis();
            long diffTime = currTime - startTime;
            if (diffTime > 5000) {
                log.warn("DB stroage exception! please wait....");
                startTime = currTime;
            }
            return; // db不正常时跳出
        }
        String uuid = "";
        // 1.处理异常时的数据
        if (!errQueue.isEmpty()) {
            uuid = errQueue.poll();
            Object records = cacheMap.get(uuid);
            if (records != null) {
                collector.emit(new Values(records), new Values(uuid, System.currentTimeMillis()));
                log.info("Recovery emit data! id: {}", uuid);
            }
            return;
        }

        // 2.拉取kafka上的数据
        List<byte[]> records = null;
        try {
            records = ConsumerClient.instance.pullRecord();
        } catch (Exception ex) {
            log.error("Pull kafka data error! id: {}", uuid, ex);
        }
        if (records == null || records.isEmpty()) {
            return;
        }
        for (byte[] bytes : records) {
            try {
                uuid = UUID.randomUUID().toString();
                collector.emit(new Values((Object) bytes), new Values(uuid, System.currentTimeMillis()));
                cacheMap.put(uuid, bytes);
                log.info("Emit data success! id: {}, length: {}", uuid, bytes.length);
            } catch (Exception ex) {
                log.error("Emit data error! id: {}", uuid, ex);
                errQueue.add(uuid); // 异常时将数据放入队列,下次处理.
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("records"));
    }

    @Override
    public void ack(Object msgId) {
        Values ackMsg = (Values) msgId;
        if (ackMsg.size() != 2) {
            return;
        }
        String uuid = ackMsg.get(0).toString();
        long emitTime = Long.parseLong(ackMsg.get(1).toString());
        cacheMap.remove(uuid);
        log.info("Ack success! id: {}, costing: {}", uuid, (System.currentTimeMillis() - emitTime));
    }

    @Override
    public void fail(Object msgId) {
        Values ackMsg = (Values) msgId;
        if (ackMsg.size() != 2) {
            return;
        }
        String uuid = ackMsg.get(0).toString();
        long emitTime = Long.parseLong(ackMsg.get(1).toString());
        String filePath = "";
        String rootPath = GlobalContext.getConfig().getRootPath();
        if (cacheMap.containsKey(uuid)) {
            filePath = KafkaGlobals.ERROR_FILE_PATH.concat(DateFormatUtils.format(new Date(), "/yyyy/MM/dd/HH"));
            FileUtils.saveErrorFile(cacheMap.get(uuid), rootPath, filePath, uuid);
            cacheMap.remove(uuid);
        }
        log.info("Ack fail! error file path: {}{}, id: {}, costing: {}", rootPath, filePath, uuid, (System.currentTimeMillis() - emitTime));
    }

    @Override
    public void close() {
        if (dbNode != null) {
            dbNode.close();
        }
        JedisClient.init().expire(RedisGlobals.CHECK_STORAGE_JOB_KEY, 1);
    }
}
