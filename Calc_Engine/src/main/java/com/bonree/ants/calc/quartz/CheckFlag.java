package com.bonree.ants.calc.quartz;

/**
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2019/1/29 15:41
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 检查标记
 * *****************************************************************************
 */
public class CheckFlag {

    /**
     * kafka发生异常的标记
     */
    private static volatile boolean KAFKA_FLAG = true;

    public static boolean getKafkaFlag() {
        return KAFKA_FLAG;
    }

    public static void setKafkaFlag(boolean kafkaFlag) {
        CheckFlag.KAFKA_FLAG = kafkaFlag;
    }

    /**
     * redis发生异常的标记
     */
    private static volatile boolean REDIS_FLAG = true;

    public static boolean getRedisFlag() {
        return REDIS_FLAG;
    }

    public static void setSetRedisFlag(boolean redisFlag) {
        CheckFlag.REDIS_FLAG = redisFlag;
    }

}
