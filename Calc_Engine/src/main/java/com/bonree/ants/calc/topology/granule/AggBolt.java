package com.bonree.ants.calc.topology.granule;

import bonree.proxy.JedisProxy;
import com.bonree.ants.calc.utils.Globals;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.granule.topology.GranuleCalcBolt;
import com.bonree.ants.commons.kafka.KafkaCommons;
import com.bonree.ants.commons.kafka.KafkaGlobals;
import com.bonree.ants.commons.kafka.ProducerClient;
import com.bonree.ants.commons.redis.JedisClient;
import com.bonree.ants.commons.utils.FileUtils;
import com.bonree.ants.plugin.etl.model.Records;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月12日 下午8:07:25
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 粒度汇总BOLT
 * ****************************************************************************
 */
public class AggBolt extends GranuleCalcBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(AggBolt.class);

    private static AtomicBoolean init = new AtomicBoolean(false);

    @Override
    @SuppressWarnings("rawtypes")
    public void prepareConf(Map conf, TridentOperationContext context) {
        // 初始kafka客户端
        if (init.compareAndSet(false, true)) {
            Globals.setLocalUniqueNo(context.getPartitionIndex());
        }
        log.info("Init agg bolt unique number: {}, {}", Globals.getLocalUniqueNo(), context.getPartitionIndex());
    }

    @Override
    public void init() {
        loadErrorFile(); // 检查错误文件
    }

    @Override
    public void calcResult(Records<Long, Object> result, String bizName, int granule, String granuleTime) {
        String resultTopic = GlobalContext.getConfig().getResultTopic();
        String resultDayTopic = GlobalContext.getConfig().getResultDayTopic();
        if (StringUtils.isNotEmpty(resultDayTopic) && GlobalContext.getConfig().tailGranule() == granule) {
            resultTopic = resultDayTopic ;
        }
        int kafkaBatchCount = GlobalContext.getConfig().getKafkaBatchCount();
        if (!super.graMap.isSkip(granule)) { // 跳过不需要存储的数据
            KafkaCommons.sendDataToKafka(result, bizName, granule, resultTopic, granuleTime, kafkaBatchCount);
        }
    }

    /**
     * 概述：检查kafka出错时保存的错误文件并发回到kafka
     */
    private void loadErrorFile() {
        JedisProxy redis = null;
        try {
            redis = JedisClient.init();
            String lockKey = KafkaGlobals.KAFKA_ERROR_FILE_KEY + Globals.getLocalUniqueNo();
            if (JedisClient.syncLock(redis, lockKey, KafkaGlobals.KAFKA_ERROR_FILE_EXPIRE)) {
                long start = System.currentTimeMillis();
                File file = new File(GlobalContext.getConfig().getRootPath().concat(KafkaGlobals.KAFKA_ERROR_PATH));
                File[] files = file.listFiles();
                if (files == null || files.length == 0) {
                    return;
                }
                byte[] fileBytes;
                for (File errFile : files) {
                    try {
                        if (errFile == null) {
                            continue;
                        }
                        fileBytes = FileUtils.file2Byte(errFile);
                        ProducerClient.instance.callbackMsg(GlobalContext.getConfig().getResultTopic(), fileBytes, KafkaGlobals.KAFKA_ERROR_PATH); // 将错误文件发送给kafka
                        if (!errFile.delete()) {// 删除文件
                            log.info("Delete file failed, path:{}, name:{} ", errFile.getPath(), errFile.getName());
                        }
                    } catch (Exception ex) {
                        log.error("checkErrorFile ", ex);
                    }
                }
                log.info("Send error file to kafka, count: {}, cost: {}", files.length, (System.currentTimeMillis() - start));
            }
        } catch (Exception e) {
            log.error("LoadErrorFile", e);
        } finally {
            JedisClient.close(redis);
        }
    }
}
