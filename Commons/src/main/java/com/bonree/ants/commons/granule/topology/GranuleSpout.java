package com.bonree.ants.commons.granule.topology;

import bonree.proxy.JedisProxy;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018/10/30 14:14
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 粒度汇总Spout的抽象父类, 主要用于约束一些业务方法的实现
 * *****************************************************************************
 */
public abstract class GranuleSpout {

    /**
     * 概述：当前拓扑处理业务的类型,时序业务或基线业务或报警业务
     *
     * @return 业务类型
     */
    protected abstract String bizType();

    /**
     * 概述：发射数据
     *
     * @param redis redis对象
     */
    protected abstract void emitData(JedisProxy redis) throws Exception;

    /**
     * 概述：处理ack信息
     */
    protected abstract void ackMsg(Values ackValue) throws Exception;

    /**
     * 概述：初始配置参数
     *
     * @param conf 配置信息集合
     */
    @SuppressWarnings("rawtypes")
    protected void initConf(Map conf) {

    }

    /**
     * 概述：发射数据校验(各粒度间是否需要等待),子类可重写此方法
     * 用法是在uploadRedisStatus方法中写入一些标记,然后在此方法是获取并做验证使用.
     *
     * @param redis redis集群连接对象
     * @param curGra      当前粒度
     * @return true 停止发射; fasle 继续发射
     */
    protected boolean emitValidate(JedisProxy redis, int curGra) {
        return false;
    }

    /**
     * 概述：更新redis中存储的状态信息,此方法会在当前批次数据执行完成后调用,子类可重写此方法
     * 用法是在此方法中写入一些标记,然后在emitValidate方法是获取并做验证使用.
     *
     * @param redis       redis集群连接对象
     * @param currGraTime 当前粒度的时间戳
     * @param curGra      当前粒度
     */
    protected void uploadRedisStatus(JedisProxy redis, String currGraTime, int curGra) throws Exception {

    }

    /**
     * 概述：发射数据前的准备工作,子类可重写此方法
     *
     * @param redis  redis集群连接对象
     * @param curGra 当前粒度
     */
    protected void emitPrepare(JedisProxy redis, String curGra) throws Exception {

    }

    /**
     * 概述：自定义业务计算
     *
     * @param granule     当前粒度
     * @param granuleTime 当前计算的粒度时间(long类型的时间戳)
     */
    protected void customCalcJob(int granule, long granuleTime) {

    }

    /**
     * 概述：指定粒度延迟计算
     *
     * @param granule     当前粒度
     * @param granuleTime 当前计算的粒度时间(long类型的时间戳)
     * @return true 延迟; fasle 不延迟
     */
    protected boolean delayCalc(int granule, long granuleTime) {
        return false;
    }

    /**
     * 概述：两次粒度汇总间隔限制,默认fasle,子类可重写此方法
     *
     * @param redis       redis集群连接对象
     * @param currGra     当前粒度
     * @param granuleTime 数据时间串
     * @return true 跳出计算; false 继续计算
     */
    protected boolean intervalLimit(JedisProxy redis, int currGra, String granuleTime) {
        return false;
    }

}
