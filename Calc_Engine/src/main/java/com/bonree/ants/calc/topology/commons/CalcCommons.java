package com.bonree.ants.calc.topology.commons;

import bonree.proxy.JedisPipelineProxy;
import bonree.proxy.JedisProxy;
import com.bonree.ants.calc.utils.FinalUtils;
import com.bonree.ants.commons.Commons;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.enums.AggType;
import com.bonree.ants.commons.enums.BizType;
import com.bonree.ants.commons.enums.DataType;
import com.bonree.ants.commons.enums.Delim;
import com.bonree.ants.commons.granule.GranuleCommons;
import com.bonree.ants.commons.granule.GranuleFinal;
import com.bonree.ants.commons.kafka.KafkaCommons;
import com.bonree.ants.commons.redis.JedisClient;
import com.bonree.ants.commons.schema.SchemaGLobals;
import com.bonree.ants.commons.schema.model.SchemaData;
import com.bonree.ants.commons.schema.model.SchemaFields;
import com.bonree.ants.commons.utils.DateFormatUtils;
import com.bonree.ants.plugin.etl.model.Records;
import com.bonree.ants.plugin.operator.IOperatorService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月10日 下午4:56:07
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 计算业务公共方法
 * ****************************************************************************
 */
public class CalcCommons {

    private static final Logger log = LoggerFactory.getLogger(CalcCommons.class);

    /**
     * 概述：指标计算-预处理
     *
     * @param baseRecords 源数据
     * @param curRecords  当前数据
     * @param schema      schema配置对象
     * @param operator    自定义算子接口
     */
    public static void calcMetric(Records<String, Object> baseRecords, Records<String, Object> curRecords, SchemaData schema, IOperatorService operator) throws Exception {
        String[] express;
        Object curValue;
        Object newValue;
        for (SchemaFields fields : schema.getFieldsList()) {
            if (!fields.getType().equals(SchemaGLobals.METRIC)) {
                continue; // 不是指标字段不需要计算
            }
            String name = fields.getName();
            DataType valueType = DataType.valueOf(fields.getValueType());
            Object oldValue = baseRecords.containsKey(name) ? baseRecords.get(name) : 0;
            try {
                express = fields.getExpr().split("\\$");
                String fun = express[0];  // 计算类型(sum,max,min等)
                if (express.length == 2 && !fun.startsWith(AggType.UDF.type())) {
                    curValue = customFunCalc(curRecords, express, valueType, fields.getExpressList(), name); // 自定义计算表达式
                } else if (AggType.UDF.type().equals(fun)) {
                    curValue = operator.udf(curRecords, valueType.tclass()); // 自定义函数
                } else if (AggType.UDF1.type().equals(fun)) {
                    curValue = operator.udf1(curRecords, valueType.tclass()); // 自定义函数
                } else if (AggType.UDF2.type().equals(fun)) {
                    curValue = operator.udf2(curRecords, valueType.tclass()); // 自定义函数
                } else if (AggType.UDF3.type().equals(fun)) {
                    curValue = operator.udf3(curRecords, valueType.tclass()); // 自定义函数
                } else if (AggType.UDF4.type().equals(fun)) {
                    curValue = operator.udf4(curRecords, valueType.tclass()); // 自定义函数
                } else if (AggType.UDF5.type().equals(fun)) {
                    curValue = operator.udf5(curRecords, valueType.tclass()); // 自定义函数
                } else {
                    curValue = curRecords.containsKey(name) ? curRecords.get(name) : 0;
                }
                if (fun.equals(AggType.HYPERLOG.type())) { // 去重
                    newValue = GranuleCommons.gatherRecordsToSet(oldValue, curValue);
                } else if (fun.startsWith(AggType.MEDIAN.type())) { // 中位数处理
                    newValue = GranuleCommons.gatherRecordsToList(oldValue, curValue);
                } else { // 按指定的聚合类型聚合指标值
                    newValue = GranuleCommons.aggregateFuntion(oldValue, curValue, valueType, fun);
                }
                baseRecords.put(name, newValue);
            } catch (Exception ex) {
                log.error("CalcMetric error! table:{}, field:{}", schema.getBizName(), name, ex);
            }
        }
    }

    /**
     * 概述：自定义函数计算
     *
     * @param records    数据对象
     * @param funExpress 计算表达式
     * @param valueType  类型(long,double)
     * @param fieldList  计算函数所需要的属性
     * @param name       属性名称
     * @return 计算结果
     */
    private static Object customFunCalc(Records<String, Object> records, String[] funExpress, DataType valueType, List<String> fieldList, String name) throws Exception {
        String fun = "";
        String expr = "";
        try {
            fun = funExpress[0];
            expr = funExpress[1];
            CustomFunction.Expression express = new CustomFunction.Expression(expr);
            if (fieldList.size() == 1) {
                String fields = fieldList.get(0);
                if (!StringUtils.isNumeric(fields)) {
                    return records.get(fields);
                }
                if (DataType.DOUBLE.equals(valueType)) {
                    return Double.parseDouble(fields);
                } else {
                    return Long.parseLong(fields);
                }
            }
            for (String fields : fieldList) {
                if (StringUtils.isNumeric(fields) || (fields.startsWith("'") && fields.endsWith("'"))) {
                    continue; // 表达式中的数字或字符串作为常量,不做替换处理
                }
                if (!records.containsKey(fields)) {
                    log.warn("Not found {} by records {}", fields, records);
                    continue;
                }
                express.put(fields, records.get(fields));
            }
            Object result = CustomFunction.eval(express.toString(), valueType); // 自定义计算函数
            if (AggType.COUNTIF.type().equals(fun)) {
                return (Boolean) result ? 1L : 0L; // 自定义case when求count函数
            } else if (fun.endsWith(AggType.IF.type())) {
                return (Boolean) result ? records.get(name) : 0L; // 自定义求maxif/minif/sumif函数
            } else {
                return result; // 自定义计算函数
            }
        } catch (Exception ex) {
            log.error("CustomFuncCalc error, fun: {}, express: {}, fieldList: {}, records: {}", fun, expr, fieldList, records);
            throw ex;
        }
    }

    /**
     * 去掉有序数据中最好和最差的5%
     *
     * @param array 有序数组
     * @return 去掉最好和最差的5%之后的有序数组
     */
    public static Object[] subArray(Object[] array) {
        // 先对数组排序
        Arrays.sort(array);
        int len = array.length;
        // 不过滤
        if (array.length < FinalUtils.FILTER_SIZE) {
            return array;
        }
        int cut = (int) (len * FinalUtils.PERCENT_5);
        Object[] destArray = new Object[len - 2 * cut];
        System.arraycopy(array, cut, destArray, 0, len - 2 * cut);
        return destArray;
    }

    /**
     * 概述：计算数组的指定分位值
     *
     * @param array   计算的数组
     * @param percent 分位值
     * @return 数组的分位值
     */
    public static Object calcPercentPoint(Object[] array, double percent) {
        int n = array.length;
        if (n <= 1) {
            return array[0];
        }
        double ij = (n - 1) * percent;
        int i = (int) ij;
        if (array[0] instanceof Integer) {
            int j = (int) (ij - i);
            return (1 - j) * (Integer) array[i] + j * (Integer) array[i + 1];
        }
        if (array[0] instanceof Long) {
            long j = (long) (ij - i);
            return (1 - j) * (Long) array[i] + j * (Long) array[i + 1];
        }
        if (array[0] instanceof Double) {
            double j = ij - i;
            return (1 - j) * (Double) array[i] + j * (Double) array[i + 1];
        }
        throw new NumberFormatException("calcPercentPoint array format error! data type: " + array[0].getClass().getName());
    }

    /**
     * 概述：保存数据到redis的hyperlog结构中
     *
     * @param result   中间结果
     * @param bizName  数据名称
     * @param bizType  数据类型
     * @param granule  粒度
     * @param time     数据时间
     * @param pipeline redis连接对象
     * @param schema   业务配置对象
     */
    @SuppressWarnings("unchecked")
    public static void saveDataToHyperLog(Records<Long, Object> result, String bizName, String bizType, int granule, String time, JedisPipelineProxy pipeline, SchemaData schema) {
        String resultKey;
        Object tmpObj = null;// 属性值
        int total = 0;
        Set<String> data;
        Records<String, Object> records;
        long startTime = System.currentTimeMillis();
        try {
            String dataKey = String.format(GranuleFinal.DATA_KEY, bizType, granule, time, bizName, 0);
            for (Map.Entry<Long, Object> entry : result.get().entrySet()) {
                long dimensionCode = entry.getKey();
                Object dimesion = null; // 目前日活计算只支持一个维度,所以此属性只会被赋一个值
                records = (Records<String, Object>) entry.getValue();
                resultKey = dataKey + Delim.COLON.value() + dimensionCode;
                for (SchemaFields fields : schema.getFieldsList()) {
                    String name = fields.getName();
                    try {
                        if (fields.getType().equals(SchemaGLobals.HIDDEN)) {
                            continue;
                        }
                        if (fields.getType().equals(SchemaGLobals.DIMENSION)) {
                            dimesion = records.get(name);
                            continue;
                        }
                        tmpObj = records.get(name);
                        if (tmpObj == null) {
                            continue;
                        }
                        data = (Set<String>) tmpObj;
                        total = data.size();
                        pipeline.pfadd(resultKey, data.toArray(new String[0]));
                        pipeline.expire(resultKey, GranuleFinal.DATA_EXPIRE);
                        break;
                    } catch (Exception ex) {
                        log.error("Hyperlog error! table:{}, field:{}, value:{}, {}", bizName, name, tmpObj, ex);
                    }
                }
                if (dimesion == null) {
                    throw new NullPointerException("Save hyperlog dimension is null!" + bizName);
                }
                pipeline.sadd(dataKey, dimesion + Delim.COLON.value() + dimensionCode); // 保存具体的业务维度,计算日活时使用.
                pipeline.expire(dataKey, GranuleFinal.DATA_EXPIRE);
            }
            log.info("Save data to hyperlog successfull! biz:{}, count:{}, cost: {}", dataKey, total, System.currentTimeMillis() - startTime);
        } catch (Exception ex) {
            log.error("Save data to hyperlog error! ", ex);
        }
    }

    /**
     * 概述：计算活跃数
     *
     * @param granule 粒度
     */
    public static Runnable calcActive(final int granule) {
        return new Runnable() {
            @Override
            public void run() {
                JedisProxy redis = null;
                String bizType = BizType.ACTIVE.type();
                try {
                    String activeCalcKey = String.format(GranuleFinal.ACTIVE_CALC_KEY, bizType, granule);
                    redis = JedisClient.init();
                    if (redis.exists(activeCalcKey)) {
                        return; // 表示正在计算中....
                    }
                    String activeKey = String.format(GranuleFinal.ACTIVE_AGG_KEY, bizType, granule);
                    long count = redis.scard(activeKey);
                    if (count < 2) {
                        return; // 表示未达到计算的条件
                    }
                    List<String> timeList = Commons.sortSet(redis.smembers(activeKey));
                    for (int n = 0; n < count - 1; n++) { // 正常只遍历一次,当有积压时会有多次
                        String time = timeList.get(n);
                        redis.set(activeCalcKey, time);
                        log.info("Start calc {} {} active number .... active list:{}", activeKey, time, timeList);
                        long granuleTime = DateFormatUtils.parse(time, GlobalContext.SEC_PATTERN).getTime();
                        Map<String, SchemaData> bizMap = SchemaGLobals.getSchemaByType(granuleTime, bizType); // 获取当前业务需要计算的业务名称集合
                        if (bizMap == null || bizMap.isEmpty()) {
                            log.warn("Calc {} active number get bizNameSet is empty !!! ", activeCalcKey);
                            return;
                        }
                        // 封装结果数据
                        Records<Long, Object> result;
                        for (Map.Entry<String, SchemaData> entry : bizMap.entrySet()) {// 遍历每种业务类型
                            result = new Records<>();
                            String bizName = entry.getKey();
                            String dataKey = String.format(GranuleFinal.DATA_KEY, bizType, granule, time, bizName, 0);
                            if (!redis.exists(dataKey)) {
                                log.info("Active number key not exists {} ....", dataKey);
                                continue;
                            }
                            calcActiveByBiz(redis, dataKey, result, entry.getValue().getFieldsList());
                            int kafkaBatchCount = GlobalContext.getConfig().getKafkaBatchCount();// 发送业务结果数据到kafka
                            String dataTopic = GlobalContext.getConfig().getResultTopic();
                            KafkaCommons.sendDataToKafka(result, bizName, granule, dataTopic, time, kafkaBatchCount);
                        }
                        redis.srem(activeKey, time);
                    }
                    redis.del(activeCalcKey);
                } catch (Exception ex) {
                    log.error("calcActive error!", ex);
                } finally {
                    JedisClient.close(redis);
                }
            }
        };
    }

    /**
     * 概述：计算每个业务的活跃数
     *
     * @param redis     redis对象
     * @param dataKey   活跃数的数据key
     * @param result    计算结果对象
     * @param fieldList 计算日活的字段集合
     */
    private static void calcActiveByBiz(JedisProxy redis, String dataKey, Records<Long, Object> result, List<SchemaFields> fieldList) throws Exception {
        Records<String, Object> records;
        long setCount = redis.scard(dataKey);
        String[] dimensionArr;
        for (int i = 0; i < setCount; i++) {
            records = new Records<>();
            String tmpDimension = redis.spop(dataKey); // 获取每个业务维度
            dimensionArr = tmpDimension.split(Delim.COLON.value());
            if (dimensionArr == null || dimensionArr.length != 2) {
                log.warn("Calc active number dimesion illegal: {}, dataKey: {}!!! ", tmpDimension, dataKey);
                continue;
            }
            String dimension = dimensionArr[0];
            String dimensionCode = dimensionArr[1];
            for (SchemaFields fields : fieldList) { // 处理维度和指标
                if (SchemaGLobals.HIDDEN.equals(fields.getType())) {
                    continue;
                }
                if (SchemaGLobals.DIMENSION.equals(fields.getType())) {
                    records.put(fields.getName(), DataType.valueOf(fields.getValueType()).convert(dimension));
                } else {
                    String resultKey = dataKey + Delim.COLON.value() + dimensionCode;
                    redis.expire(resultKey, 1);
                    records.put(fields.getName(), redis.pfcount(resultKey));
                }
            }
            result.put((long) i, records);
        }
        redis.expire(dataKey, 1);
    }
}
