package com.bonree.ants.commons.granule;

import bonree.proxy.JedisProxy;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.enums.AggType;
import com.bonree.ants.commons.enums.DataType;
import com.bonree.ants.commons.enums.Delim;
import com.bonree.ants.commons.redis.RedisGlobals;
import com.bonree.ants.commons.schema.SchemaGLobals;
import com.bonree.ants.commons.schema.model.SchemaData;
import com.bonree.ants.commons.schema.model.SchemaFields;
import com.bonree.ants.commons.utils.DateFormatUtils;
import com.bonree.ants.commons.utils.Md5Utils;
import com.bonree.ants.plugin.etl.model.Records;
import com.bonree.ants.plugin.storage.model.DataResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.*;
import java.util.Map.Entry;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年8月16日 下午5:52:18
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 粒度汇总公共方法
 * ****************************************************************************
 */
public class GranuleCommons {

    private static final Logger log = LoggerFactory.getLogger(GranuleCommons.class);

    /**
     * 概述：控制是否发送数据(处理拓扑间的计算依赖)
     *
     * @param redis        redis对象
     * @param lastEmitTime 最后一次发送数据的时间
     * @param topoName     拓扑名称
     * @param timeout      单位:秒,最大的计算超时时间,超过这个时间所对应的粒度计算就会停止
     */
    public static boolean emitController(JedisProxy redis, long lastEmitTime, String topoName, long timeout) {
        try {
            String calcPositionKey = String.format(GranuleFinal.CALC_POSITION_KEY, topoName);
            if (!redis.exists(calcPositionKey) && redis.ttl(calcPositionKey) == -2) { // 表示key不存在
                log.info("{} calc stoped !!! please waiting...", topoName);
                return true;
            }
            if (lastEmitTime == 0 || timeout == 0) {
                return false;
            }
            String calcPosition = redis.get(calcPositionKey);
            long time = lastEmitTime / 1000 * 1000;
            long timeDiff = time - DateFormatUtils.getTime(calcPosition, GlobalContext.SEC_PATTERN);
            if (timeDiff / 1000 > timeout) {
                String recordTime = DateFormatUtils.format(new Date(lastEmitTime), GlobalContext.SEC_PATTERN);
                log.info("{} calc slowly {} sec, please waiting... recordTime: {}, currCalcPosition: {}", topoName, timeout, recordTime, calcPosition);
                return true;
            }
        } catch (Exception e) {
            log.error("EmitController error !!!", e);
        }
        return false;
    }

    /**
     * 概述：粒度汇总控制器
     *
     * @param redis    redis对象
     * @param bizType  汇总数据的业务类型
     * @param nextGra  下一个汇总粒度
     * @param currTime 当前汇总的时间串(yyyyMMddHHmmss)
     */
    public static void granuleController(JedisProxy redis, String bizType, int nextGra, String currTime) {
        try {
            String granuleKey = String.format(GranuleFinal.AGG_TIME_KEY, bizType, nextGra);  // 下一粒度汇总KEY
            String nextGraTime = granuleCalcStr(nextGra, currTime); // 计算出下一粒度时间戳
            if (redis.hexists(granuleKey, nextGraTime)) {
                String timeStr = redis.hget(granuleKey, nextGraTime);
                if (timeStr.startsWith(RedisGlobals.CALCED_FLAG)) {
                    return;// 丢弃掉已经发送过的粒度
                }
                if (timeStr.contains(currTime)) {
                    return;// 重复时间串处理
                }
                currTime = timeStr + "," + currTime;
            }
            Map<String, String> map = new HashMap<>();
            map.put(nextGraTime, currTime);
            // 粒度的时间戳集合: hm< key:下一个汇总粒度, value:<key:下一粒度时间戳(由value计算得出), value:下一粒度汇总时所需要的时间戳 >>
            // 以一分钟为例:
            // hm<"MIN_$60$_AGG_KEY", <key:201604121210, value:201604121210,201604121211,201604121212,201604121213,201604121214>>
            redis.hmset(granuleKey, map);
            log.info("Save timeKey to redis, nextGraKey: {}, nextGraTime: {}", granuleKey, map);
        } catch (Exception ex) {
            log.error("GranuleController error! currTime: {}, Exception: {}", currTime, ex);
        }
    }

    /**
     * 概述：检查当前粒度时间是否已经汇总过
     *
     * @param redis    redis对象
     * @param bizType  汇总数据的业务类型
     * @param nextGra  下一个汇总粒度
     * @param currTime 当前汇总的时间串(yyyyMMddHHmmss)
     * @return 为true时表示已经汇总过, 否则表示未汇总.
     */
    public static boolean checkCalced(JedisProxy redis, String bizType, int nextGra, long currTime) throws Exception {
        String nextGraTime = granuleCalcStr(nextGra, currTime); // 计算出下一粒度时间戳
        String calcStatusKey = String.format(GranuleFinal.GRANULE_CALC_STATUS_KEY, bizType, nextGraTime);
        return redis.exists(calcStatusKey);
    }

    /**
     * 概述：校正粒度数据超时时间
     *
     * @param granule   当前所汇总的粒度
     * @param upGranule 汇总当前粒度所需要的数据粒度,例 当前汇总的粒度是60,那么所需要的数据粒度是10
     * @param strtime   汇总数据的粒度时间
     * @return 校正后的超时时间, 单位:毫秒
     */
    public static String correctTimeout(long granule, long upGranule, String strtime) {
        if (strtime == null || "".equals(strtime)) {
            return "0";
        }
        long value = 0;
        try {
            long time = DateFormatUtils.parse(strtime, GlobalContext.SEC_PATTERN).getTime();
            if (granule > 1 && granule <= 60) { // 1-60秒粒度
                int sec = DateFormatUtils.getSeconds(time);
                value = sec % granule;
            } else if (granule > 60 && granule <= 3600) {// 2分钟-1小时粒度
                int min = DateFormatUtils.getMinute(time);
                value = min * 60 % granule;
            } else if (granule > 3600 && granule <= 86400) {// 2-23小时粒度
                int hour = DateFormatUtils.getHour(time);
                value = hour * 60 * 60 % granule;
            } else if (granule >= 86400) {// 天粒度
                value = DateFormatUtils.getHour(time) * 60 * 60;
            }
        } catch (ParseException ex) {
            log.error("CheckTimeout error!", ex);
        }
        return String.valueOf((value + upGranule) * 1000);// 转换为毫秒
    }

    /**
     * 概述： 数据聚合
     *
     * @param data     预聚合后的数据
     * @param result   结果数据
     * @param isGather 是否收集原始数据,用于中位数计算. true 收集(预处理拓扑时为true), false不收集(粒度汇总拓扑)
     */
    @SuppressWarnings("unchecked")
    public static void aggregateData(DataResult data, Map<String, Records<Long, Object>> result, boolean isGather) throws Exception {
        String bizName = data.getBizName();
        String bizType = data.getBizType();
        long time = Long.parseLong(data.getTime());
        SchemaData schema = SchemaGLobals.getSchemaByName(time, bizName);
        if (schema == null) {
            log.warn("CalcData not found Schema info!!! bizName: {}", bizName);
            return;
        }
        String dimension = bizType + Delim.ASCII_159.value() + bizName;
        if (!result.containsKey(dimension)) {
            result.put(dimension, new Records<Long, Object>()); // 第一层是每个业务
        }
        Records<String, Object> oldRecords;
        Records<Long, Object> cacheMap = result.get(dimension);
        for (Records<String, Object> records : data.get()) { // 计算每行数据
            String dataDimension = initDimension(records, schema).toValueString(); // 获取需要计算的维度
            long dimensionCode = Md5Utils.stringTolong(Md5Utils.getMD5Str(dataDimension));
            if (!cacheMap.containsKey(dimensionCode)) {
                cacheMap.put(dimensionCode, cloneRecords(records)); // 第二层是维度
                continue;
            }
            oldRecords = (Records<String, Object>) cacheMap.get(dimensionCode);
            // 计算每一列指标值
            calcMetric(oldRecords, records, schema, isGather);
        }
    }

    /**
     * 概述：指标计算-汇总
     *
     * @param oldRecords 源数据
     * @param curRecords 当前数据
     * @param schema     schema配置对象
     * @param isGather   是否收集原始数据
     */
    private static void calcMetric(Records<String, Object> oldRecords, Records<String, Object> curRecords, SchemaData schema, boolean isGather) {
        if (schema == null) {
            return;
        }
        String[] express;
        Object newValue;
        for (SchemaFields fields : schema.getFieldsList()) {
            if (fields.getType().equals(SchemaGLobals.DIMENSION)) {
                continue; // 维度字段不需要计算
            }
            String name = fields.getName();
            DataType valueType = DataType.valueOf(fields.getValueType());
            Object oldValue = oldRecords.containsKey(name) ? oldRecords.get(name) : 0;
            try {
                express = fields.getExpr().split("\\$");
                String fun = express[0];  // 计算类型(sum,max,min等)
                Object curValue = curRecords.containsKey(name) ? curRecords.get(name) : 0;
                if (isGather && fun.equals(AggType.HYPERLOG.type())) { // 去重
                    newValue = gatherRecordsToSet(oldValue, curValue);
                } else if (isGather && fun.startsWith(AggType.MEDIAN.type())) { // 中位数处理
                    newValue = gatherRecordsToList(oldValue, curValue);
                } else { // 按指定的聚合类型聚合指标值
                    newValue = aggregateFuntion(oldValue, curValue, valueType, fun);
                }
                oldRecords.put(name, newValue);
            } catch (Exception ex) {
                log.error("CalcMetric1 error! table:{}, field:{}", schema.getBizName(), name, ex);
            }
        }
    }

    /**
     * 概述：数据聚合函数
     *
     * @param oldValue  旧值
     * @param curValue  新值
     * @param valueType 值类型
     * @param fun       计算方式
     * @return 聚合结果
     */
    public static Object aggregateFuntion(Object oldValue, Object curValue, DataType valueType, String fun) {
        Object value = 0L;
        if (DataType.LONG.equals(valueType)) {
            long old = valueType.convert(oldValue);
            long cur = valueType.convert(curValue);
            if (fun.startsWith(AggType.SUM.type()) || fun.startsWith(AggType.COUNT.type()) || fun.equals(AggType.MEDIAN.type())) {
                value = old + cur;
            } else if (fun.startsWith(AggType.MAX.type()) || fun.equals(AggType.MEDIAN90.type())) {
                value = old > cur ? old : cur;
            } else if (fun.startsWith(AggType.MIN.type())) {
                value = old < cur && old != 0 ? old : cur;
            }
        } else if (DataType.DOUBLE.equals(valueType)) {
            double old = valueType.convert(oldValue);
            double cur = valueType.convert(curValue);
            if (fun.startsWith(AggType.SUM.type()) || fun.startsWith(AggType.COUNT.type()) || fun.equals(AggType.MEDIAN.type())) {
                value = old + cur;
            } else if (fun.startsWith(AggType.MAX.type()) || fun.equals(AggType.MEDIAN90.type())) {
                value = old > cur ? old : cur;
            } else if (fun.startsWith(AggType.MIN.type())) {
                value = old < cur && old != 0.0 ? old : cur;
            }
        } else if (DataType.TIMESTAMP.equals(valueType)) {
            long old = valueType.convert(oldValue);
            long cur = valueType.convert(curValue);
            if (fun.startsWith(AggType.MAX.type())) {
                value = old > cur ? old : cur;
            } else if (fun.startsWith(AggType.MIN.type())) {
                value = old < cur && old != 0 ? old : cur;
            }
        }
        return value;
    }

    /**
     * 概述：初始record对象中的维度参数
     *
     * @param records 数据对象
     * @param schema  schema配置对象
     */
    public static Records<String, Object> initDimension(Records<String, Object> records, SchemaData schema) {
        if (schema == null || records == null) {
            return null;
        }
        Records<String, Object> tmpRecords = new Records<>();
        for (SchemaFields data : schema.getFieldsList()) {
            if (SchemaGLobals.DIMENSION.equals(data.getType())) {
                tmpRecords.put(data.getName(), records.get(data.getName()));
            }
        }
        return tmpRecords;
    }

    /***
     * 概述：粒度计算
     * @param granule 粒度
     * @param time    时间(yyyyMMddHHmmss)
     * @return string
     */
    public static String granuleCalcStr(int granule, String time) throws Exception {
        long tempTime = DateFormatUtils.parse(time, GlobalContext.SEC_PATTERN).getTime();
        return granuleCalcStr(granule, tempTime);
    }

    /***
     * 概述：粒度计算
     * @param granule 粒度
     * @param time    long类型的时间戳
     */
    public static String granuleCalcStr(int granule, long time) {
        long times = granuleCalc(granule, time).getTime();
        return DateFormatUtils.format(new Date(times), GlobalContext.SEC_PATTERN);
    }

    /**
     * 概述： 粒度时间计算
     *
     * @param granule 粒度
     * @param time    long类型的时间戳
     */
    public static Date granuleCalc(long granule, long time) {
        int tempTime;
        int value;
        Date date = new Date(time);
        granule = granule / 60;
        if (granule == 1) { // 1分钟
            tempTime = DateFormatUtils.getMinute(time);
            date = DateFormatUtils.setMinute(time, tempTime);
        } else if (granule > 1 && granule < 60) { // 2-59分钟粒度
            tempTime = DateFormatUtils.getMinute(time);
            value = (int) (tempTime / granule * granule);
            date = DateFormatUtils.setMinute(time, value);
        } else if (granule >= 60 && granule < 1440) {// 1-23小时粒度
            tempTime = DateFormatUtils.getHour(time) * 60;
            value = (int) (tempTime / granule * granule);
            date = DateFormatUtils.setHour(time, value);
        } else if (granule >= 1440) {// 天粒度
            tempTime = DateFormatUtils.getDay(time) * 24 * 60;
            value = (int) (tempTime / granule * granule);
            date = DateFormatUtils.setDay(time, value);
        }
        return date;
    }

    /**
     * 概述：克隆Records对象
     *
     * @param records 数据对象
     */
    private static Records<String, Object> cloneRecords(Records<String, Object> records) {
        if (records == null) {
            return null;
        }
        Records<String, Object> tmpRecords = new Records<>();
        Map<String, ?> map = records.get();
        for (Entry<String, ?> entry : map.entrySet()) {
            tmpRecords.put(entry.getKey(), entry.getValue());
        }
        return tmpRecords;
    }

    /**
     * 概述：检查计算位置并设置key的过期时间
     *
     * @param redis          redis对象
     * @param topoName       拓扑名称
     * @param checkKeyExists 是否检查key, true 检查不存在后再赋值, false 直接覆盖
     */
    public static void setCalcPosition(JedisProxy redis, String topoName, boolean checkKeyExists) {
        String positionKey = String.format(GranuleFinal.CALC_POSITION_KEY, topoName);
        if (!checkKeyExists || !redis.exists(positionKey)) {
            redis.set(positionKey, DateFormatUtils.format(new Date(), GlobalContext.SEC_PATTERN));
            redis.expire(positionKey, RedisGlobals.CALC_POSITION_EXPIRE);
        } else if (redis.ttl(positionKey) < RedisGlobals.CALC_POSITION_EXPIRE / 5) {
            redis.expire(positionKey, RedisGlobals.CALC_POSITION_EXPIRE);
        }
    }

    /**
     * 概述：收集原始数据-List集合
     */
    @SuppressWarnings("unchecked")
    public static <T> T gatherRecordsToList(T oldValue, T curValue) {
        List<T> tmpObj;
        if (oldValue instanceof List<?>) {
            tmpObj = (List<T>) oldValue;
        } else {
            tmpObj = new ArrayList<>();
        }
        if (curValue instanceof List<?>) {
            tmpObj.addAll((List<T>) curValue);
        } else {
            tmpObj.add(curValue);
        }
        return (T) tmpObj;
    }

    /**
     * 概述：收集原始数据-Set集合
     */
    @SuppressWarnings("unchecked")
    public static <T> T gatherRecordsToSet(T oldValue, T curValue) {
        Set<T> tmpObj;
        if (oldValue instanceof Set<?>) {
            tmpObj = (Set<T>) oldValue;
        } else {
            tmpObj = new HashSet<>();
        }
        if (curValue instanceof Set<?>) {
            tmpObj.addAll((Set<T>) curValue);
        } else {
            tmpObj.add(curValue);
        }
        return (T) tmpObj;
    }

}
