package com.bonree.ants.storage.topology;

import com.bonree.ants.commons.Commons;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.kafka.KafkaGlobals;
import com.bonree.ants.commons.kafka.ProducerClient;
import com.bonree.ants.commons.plugin.PluginGlobals;
import com.bonree.ants.commons.plugin.PluginManager;
import com.bonree.ants.commons.plugin.model.StoragePlugin;
import com.bonree.ants.commons.redis.RedisGlobals;
import com.bonree.ants.commons.schema.SchemaGLobals;
import com.bonree.ants.commons.schema.model.SchemaData;
import com.bonree.ants.commons.utils.DateFormatUtils;
import com.bonree.ants.commons.utils.KryoUtils;
import com.bonree.ants.plugin.etl.model.CustomData;
import com.bonree.ants.plugin.storage.IStorageService;
import com.bonree.ants.plugin.storage.model.DataResult;
import com.bonree.ants.storage.business.InsertDataService;
import com.bonree.ants.storage.plugin.StoragePluginImpl;
import com.bonree.ants.storage.utils.DBUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

public class StorageBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 1L;
    private Logger log = LoggerFactory.getLogger(StorageBolt.class);

    /**
     * 存放插件初始完成前接收到的数据
     */
    private Queue<Object> tmpRecordsQueue = new ConcurrentLinkedDeque<>();
    private static final int QUEUE_SIZE_LIMIT = 2000;
    private static AtomicBoolean exists = new AtomicBoolean(false);

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {
        // 初始配置参数
        Commons.initAntsSchema(stormConf);

        if (GlobalContext.getConfig().useStoragePlugin()) {
            initStoragePlugin(); // 初始插件
        } else {
            DBUtils.init(); // 初始db连接参数
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        byte[] records = null;
        IStorageService storage;

        try {
            records = input.getBinaryByField("records");
            if (StringUtils.isNotEmpty(GlobalContext.getConfig().getCustomStorageTopic())) {
                CustomData data = KryoUtils.readFromByteArray(records); // 自定义数据存储插件
                StoragePlugin plugin = PluginGlobals.get(data.getTime(), PluginGlobals.STORAGE_QUEUE);
                if (pluginLoadComplete(plugin, data)) {
                    log.info("Custom stroage plugin init laoding .... size: {}", tmpRecordsQueue.size());
                    return;
                }
                storage = plugin.getStorageService();
                while (!tmpRecordsQueue.isEmpty()) {
                    storage.message((CustomData) tmpRecordsQueue.poll());
                }
            } else {
                DataResult data = KryoUtils.readFromByteArray(records); // 时序指标数据存储插件
                long time = DateFormatUtils.parse(data.getTime(), GlobalContext.SEC_PATTERN).getTime();
                if (GlobalContext.getConfig().useStoragePlugin()) { // 使用时序指标存储插件
                    StoragePlugin plugin = PluginGlobals.get(time, PluginGlobals.STORAGE_QUEUE);
                    if (pluginLoadComplete(plugin, data)) {
                        log.info("Stroage plugin init laoding .... size: {}", tmpRecordsQueue.size());
                        return;
                    }
                    storage = plugin.getStorageService();
                } else {
                    SchemaData schema = SchemaGLobals.getSchemaByName(time, data.getBizName());
                    storage = new InsertDataService(schema, time); // 时序指标数据默认存储数据
                    tmpRecordsQueue.add(data);
                }
                while (!tmpRecordsQueue.isEmpty()) {
                    storage.message((DataResult) tmpRecordsQueue.poll());
                }
            }
        } catch (SQLException ex) {
            connectionExcetionProcess(records);
        } catch (Exception ex) {
            log.error("storage bolt save data error! ", ex);
            throw new FailedException("Insert execute error!");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    /**
     * 概述：数据库出现连接异常时的处理
     */
    private void connectionExcetionProcess(byte[] records) {
        try {
            // 将当前数据发回kafka
            if (records != null) {
                ProducerClient.instance.callbackMsg(KafkaGlobals.CONNECTION_FAIL_TOPIC, records, KafkaGlobals.ERROR_FILE_PATH);
            }
            log.warn("Insert bolt connection error! current data send kafka!!! ");
        } catch (Exception ex) {
            log.error("Connection excetion!", ex);
        }
    }

    /**
     * 概述：加载插件
     */
    private void initStoragePlugin() {
        try {
            if (exists.compareAndSet(false, true)) {
                String pluginKey = RedisGlobals.STORAGE_PLUGIN_KEY + "_" + GlobalContext.getConfig().getCustomStorageTopic();
                String storagePluginName = GlobalContext.getConfig().getStoragePluginName();
                String storagePluginImpl = GlobalContext.getConfig().getStorageServiceImpl();
                PluginManager plugin = new StoragePluginImpl(storagePluginName, storagePluginImpl, pluginKey);
                StoragePlugin storagePlugin = plugin.load(false);
                plugin.savePlugin(storagePlugin);
            }
        } catch (Exception ex) {
            log.error("Storage plugin error! ", ex);
        }
    }

    /**
     * 判断插件是否加载完成
     *
     * @param plugin 插件对象
     * @param data   数据内存
     * @return true: 未加载完成; false:加载完成.
     */
    private boolean pluginLoadComplete(StoragePlugin plugin, Object data) {
        if (plugin == null) {
            if (tmpRecordsQueue.size() < QUEUE_SIZE_LIMIT) {
                tmpRecordsQueue.add(data);
            }
            log.info("Stroage plugin init laoding .... size: {}", tmpRecordsQueue.size());
            return true;
        }
        tmpRecordsQueue.add(data);
        return false;
    }
}
