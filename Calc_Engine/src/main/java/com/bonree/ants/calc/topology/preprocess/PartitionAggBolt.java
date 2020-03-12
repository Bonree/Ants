package com.bonree.ants.calc.topology.preprocess;

import com.bonree.ants.calc.plugin.OperatorPluginImpl;
import com.bonree.ants.calc.topology.commons.CalcCommons;
import com.bonree.ants.commons.Commons;
import com.bonree.ants.commons.enums.Delim;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.granule.GranuleCommons;
import com.bonree.ants.commons.plugin.PluginGlobals;
import com.bonree.ants.commons.plugin.PluginManager;
import com.bonree.ants.commons.plugin.model.OperatorPlugin;
import com.bonree.ants.commons.schema.SchemaGLobals;
import com.bonree.ants.commons.schema.model.SchemaData;
import com.bonree.ants.commons.utils.KryoUtils;
import com.bonree.ants.plugin.etl.model.DataSet;
import com.bonree.ants.plugin.etl.model.Records;
import com.bonree.ants.plugin.operator.IOperatorService;
import com.bonree.ants.plugin.storage.model.DataResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

public class PartitionAggBolt extends BaseAggregator<Map<String, Records<String, Object>>> {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(PartitionAggBolt.class);
    private long startTime;
    private static AtomicBoolean exists = new AtomicBoolean(false);

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map conf, TridentOperationContext context) {
        // 初始配置参数
        Commons.initAntsSchema(conf);
        if (GlobalContext.getConfig().useOperatorPlugin()) {
            initOperatorPlugin();
        }
    }

    @Override
    public Map<String, Records<String, Object>> init(Object batchId, TridentCollector collector) {
        this.startTime = System.currentTimeMillis();
        // KEY:业务数据类型, VALUE: < KEY:分组KEY, VALUE:业务数据(records对象) >
        return new HashMap<>();
    }

    @Override
    public void aggregate(Map<String, Records<String, Object>> cacheMap, TridentTuple tuple, TridentCollector collector) {
        try {
            long time = tuple.getLongByField("etime");
            List<DataSet> dataSetList = (List<DataSet>) tuple.getValueByField("dataSet");
            IOperatorService operatorService = null;
            if (GlobalContext.getConfig().useOperatorPlugin()) {
                OperatorPlugin plugin = PluginGlobals.get(time, PluginGlobals.OPERATOR_QUEUE);
                operatorService = plugin.getOperatorService();
            }
            for (DataSet data : dataSetList) {
                preCalcData(time, data, cacheMap, operatorService); // 预计算各业务数据
            }
        } catch (Exception ex) {
            log.error("PartitionAggBolt error! ", ex);
        }
    }

    @Override
    public void complete(Map<String, Records<String, Object>> cacheMap, TridentCollector collector) {
        String[] tmpArr;
        for (Entry<String, Records<String, Object>> biz : cacheMap.entrySet()) {
            String bizKey = biz.getKey();
            try {
                Collection<Object> tempObj = biz.getValue().get().values();
                tmpArr = bizKey.split(Delim.ASCII_159.value());
                DataResult data = new DataResult();
                data.setBizType(tmpArr[0]);
                data.setBizName(tmpArr[1]);
                data.setTime(tmpArr[2]); // long类型的字符串时间(例:1526007600000)
                data.setGranule(GlobalContext.getConfig().getSourceGranule());
                for (Object obj : tempObj) {
                    data.add((Records<String, Object>) obj);
                }
                byte[] compress = Snappy.compress(KryoUtils.writeToByteArray(data));
                collector.emit(new Values(bizKey, compress)); // 发送到下层全局聚合bolt, bizType的值是业务类型+业务时间的格式
            } catch (Exception ex) {
                log.error("Partition complete error! {}", bizKey, ex);
            }
        }
        GlobalContext.LOG.debug("Preprocess partition complete! cost:{}", (System.currentTimeMillis() - this.startTime));
    }

    /**
     * 概述： 数据预计算
     *
     * @param time            当前数据包的时间
     * @param source          etl后的源数据
     * @param result          预计算后的结果数据
     * @param operatorService 自定义算子接口
     */
    private void preCalcData(long time, DataSet source, Map<String, Records<String, Object>> result, IOperatorService operatorService) {
        Records<String, Object> cacheMap;
        Records<String, Object> baseRecords;
        Records<String, Object> tmpRecords;
        SchemaData schema;
        for (Entry<String, List<Records<String, Object>>> entry : source.get().entrySet()) { // 计算每个业务下的数据集合
            String bizName = entry.getKey(); // 业务类型
            try {
                schema = SchemaGLobals.getSchemaByName(time, bizName);
                if (schema == null) {
                    log.info("CalcData not found Schema info!!! bizName: {}, time: {}", bizName, time);
                    continue;
                }
                String bizDimension = schema.getBizType() + Delim.ASCII_159.value() + bizName + Delim.ASCII_159.value() + time; // 按业务类型+业务名称+时间进行分组
                if (!result.containsKey(bizDimension)) {
                    result.put(bizDimension, new Records<String, Object>());
                }
                cacheMap = result.get(bizDimension);
                for (Records<String, Object> records : entry.getValue()) { // 计算每行数据
                    tmpRecords = GranuleCommons.initDimension(records, schema);
                    String dataDimension = tmpRecords.toValueString();// 获取需要计算的维度
                    if (StringUtils.isEmpty(dataDimension)) {
                        dataDimension = time + "";
                    }
                    if (!cacheMap.containsKey(dataDimension)) {
                        cacheMap.put(dataDimension, tmpRecords);
                    }
                    baseRecords = (Records<String, Object>) cacheMap.get(dataDimension);
                    // 计算当前业务下每列指标
                    CalcCommons.calcMetric(baseRecords, records, schema, operatorService);
                }
            } catch (Exception ex) {
                log.error("PreCalcData error! bizName: {}", bizName, ex);
            }
        }
    }

    /**
     * 概述：加载算子插件
     */
    private void initOperatorPlugin() {
        try {
            if (exists.compareAndSet(false, true)) {
                PluginManager plugin = new OperatorPluginImpl(GlobalContext.getConfig().getOperatorPluginName(), GlobalContext.getConfig().getOperatorServiceImpl());
                OperatorPlugin operatorPlugin = plugin.load(false);
                plugin.savePlugin(operatorPlugin);
            }
        } catch (Exception ex) {
            log.error("Init operator plugin error! ", ex);
        }
    }
}
