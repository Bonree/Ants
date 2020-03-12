package com.bonree.ants.calc.topology.preprocess;

import com.bonree.ants.calc.plugin.EtlPluginImpl;
import com.bonree.ants.commons.Commons;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.kafka.KafkaGlobals;
import com.bonree.ants.commons.kafka.ProducerClient;
import com.bonree.ants.commons.plugin.PluginGlobals;
import com.bonree.ants.commons.plugin.PluginManager;
import com.bonree.ants.commons.plugin.model.EtlPlugin;
import com.bonree.ants.commons.utils.KryoUtils;
import com.bonree.ants.plugin.etl.IEtlService;
import com.bonree.ants.plugin.etl.model.DataSet;
import com.bonree.ants.plugin.etl.model.SchemaConfig;
import com.bonree.ants.plugin.etl.model.CustomData;
import com.bonree.ants.plugin.storage.model.DataResult;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

public class EtlBolt extends BaseFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(EtlBolt.class);
    private static final int QUEUE_SIZE_LIMIT = 2000;
    private static AtomicBoolean exists = new AtomicBoolean(false);

    /**
     * 存放插件初始完成前接收到的数据
     */
    private Queue<byte[]> tmpRecordsQueue = new ConcurrentLinkedDeque<>();

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        // 初始化参数
        Commons.initAntsSchema(conf);
        if (exists.compareAndSet(false, true)) {
            // 初始化加载etl插件
            initEtlPlugin();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(TridentTuple tuple, TridentCollector collector) {
        List<byte[]> recordsList;
        try {
            recordsList = (List<byte[]>) tuple.getValueByField("records");
            if (recordsList == null || recordsList.isEmpty()) {
                return;
            }
            long time = tuple.getLongByField("time");
            EtlPlugin plugin = PluginGlobals.get(time, PluginGlobals.ETL_QUEUE);
            if (plugin == null) {
                if (tmpRecordsQueue.size() < QUEUE_SIZE_LIMIT) {
                    tmpRecordsQueue.addAll(recordsList);
                }
                log.info("Etl plugin init laoding .... tmpSize: {}, size: {}", tmpRecordsQueue.size(), recordsList.size());
                return;
            }
            while (!tmpRecordsQueue.isEmpty()) {
                recordsList.add(tmpRecordsQueue.poll());
            }
            IEtlService etlService = plugin.getEtlService();
            SchemaConfig config = plugin.getConfig();
            Map<Long, List<DataSet>> recordsMap = new HashMap<>();
            DataSet data = null;
            int count = 0;
            long start = System.currentTimeMillis();
            for (byte[] bytes : recordsList) {
                data = etlService.onMessage(bytes, config);
                if (data == null) {
                    log.warn("Etl data is null or empty! {}", time);
                    continue;
                }
                long dataTime = Commons.getIntervalTime(data.getTimestamp(), GlobalContext.getConfig().getSourceGranule()).getTime();
                if (!recordsMap.containsKey(dataTime)) {
                    recordsMap.put(dataTime, new ArrayList<DataSet>());
                }
                recordsMap.get(dataTime).add(data);
                count++;
            }
            long costing = System.currentTimeMillis() - start;
            GlobalContext.LOG.debug("Data set: {}", data);
            for (Entry<Long, List<DataSet>> entry : recordsMap.entrySet()) {
                collector.emit(new Values(entry.getKey(), entry.getValue())); // 发送到下层预计算bolt
            }
            log.info("Etl data complete, time:{}, before count: {}, after count: {}, costing: {}", recordsMap.keySet(), recordsList.size(), count, costing);
            sendCustomData(recordsMap);  // 发送自定义数据到kafka
            sendDataResult(recordsMap);  // 发送不需要粒度汇总的数据
        } catch (Exception ex) {
            log.error("EtlBolt error!", ex);
        }
    }

    /**
     * 概述：发送自定义数据到kafka
     *
     * @param recordsMap 数据集合
     */
    private void sendCustomData(Map<Long, List<DataSet>> recordsMap) {
        if (recordsMap == null || recordsMap.isEmpty()) {
            return;
        }
        Map<String, CustomData> cacheMap = new HashMap<>();
        CustomData customData;
        String topic;
        for (Entry<Long, List<DataSet>> entry : recordsMap.entrySet()) {
            for (DataSet data : entry.getValue()) { // 遍历每种业务类型的数据
                customData = data.getCustomData();
                if (customData == null || customData.getDataList().isEmpty()) {
                    continue;
                }
                for (CustomData.File file : customData.getDataList()) {
                    try {
                        topic = file.getContextType();
                        if (!cacheMap.containsKey(topic)) {
                            cacheMap.put(topic, new CustomData(entry.getKey()));
                        }
                        cacheMap.get(topic).addData(file);
                    } catch (Exception ex) {
                        log.error("Prcess custom data error!", ex);
                    }
                }
            }
        }
        for (Entry<String, CustomData> entry : cacheMap.entrySet()) {
            try {
                byte[] bytes = KryoUtils.writeToByteArray(entry.getValue());
                ProducerClient.instance.callbackMsg(entry.getKey(), bytes, KafkaGlobals.KAFKA_SNAPSHOT_ERROR_PATH);
                log.info("Send customData to kafka.topic: {}, count: {}", entry.getKey(), entry.getValue().getDataList().size());
            } catch (Exception ex) {
                log.info("Send customData to kafka error! ", ex);
            }
        }
    }

    /**
     * 概述：发送结果数据(不需汇总的数据)到kafka
     *
     * @param recordsMap 数据集合
     */
    private void sendDataResult(Map<Long, List<DataSet>> recordsMap) {
        if (recordsMap == null || recordsMap.isEmpty()) {
            return;
        }
        Map<String, DataResult> resultMap;
        DataResult result;
        String resultTopic = GlobalContext.getConfig().getResultTopic();
        for (Entry<Long, List<DataSet>> entry : recordsMap.entrySet()) {
            for (DataSet data : entry.getValue()) { // 遍历每种业务类型的数据
                resultMap = data.getResult();
                if (resultMap == null || resultMap.isEmpty()) {
                    continue;
                }
                for (Entry<String, DataResult> map : resultMap.entrySet()) {
                    result = map.getValue();
                    if (result == null || result.get().isEmpty()) {
                        continue;
                    }
                    try {
                        byte[] bytes = KryoUtils.writeToByteArray(result);
                        ProducerClient.instance.callbackMsg(resultTopic, bytes, KafkaGlobals.KAFKA_ERROR_PATH);
                        log.info("Send data result to kafka.topic: {}, table: {}, count: {}", resultTopic, map.getKey(), result.get().size());
                    } catch (Exception ex) {
                        log.info("Send data result to kafka error! ", ex);
                    }
                }
            }
        }
    }

    /**
     * 概述：加载插件
     */
    private void initEtlPlugin() {
        try {
            PluginManager plugin = new EtlPluginImpl(GlobalContext.getConfig().getEtlPluginName(), GlobalContext.getConfig().getEtlServiceImpl());
            EtlPlugin etlPlugin = plugin.load(false);
            plugin.savePlugin(etlPlugin);
        } catch (Exception ex) {
            log.error("Init etl plugin error! ", ex);
        }
    }
}
