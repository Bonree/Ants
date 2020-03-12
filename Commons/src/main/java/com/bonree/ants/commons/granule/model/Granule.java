package com.bonree.ants.commons.granule.model;

import java.io.Serializable;

/**
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018/12/12 10:55
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 粒度相关的属性
 * *****************************************************************************
 */
public class Granule implements Serializable {

    private int granule;    // 当前粒度
    private int last;       // 当前粒度的上一个粒度
    private int next;       // 当前粒度的下一下粒度
    private int bucket;     // 当前粒度保存到redis时的分桶数量
    private int lastBucket; // 当前粒度保存到redis时上一个粒度的分桶数量
    private int nextBucket; // 当前粒度保存到redis时下一个粒度的分桶数量
    private int parallel;   // 当前粒度在汇总时的并行度(实际是一个粒度的并行度)
    private int timeout;    // 当前粒度在汇总时等待的超时时间
    private boolean skip;   // 当前粒度是否不需要进行存储, true 需要; false 不需要

    public int getGranule() {
        return granule;
    }

    public void setGranule(int granule) {
        this.granule = granule;
    }

    public int getBucket() {
        return bucket;
    }

    public void setBucket(int bucket) {
        this.bucket = bucket;
    }

    public int getLastBucket() {
        return lastBucket;
    }

    public void setLastBucket(int lastBucket) {
        this.lastBucket = lastBucket;
    }

    public int getNextBucket() {
        return nextBucket;
    }

    public void setNextBucket(int nextBucket) {
        this.nextBucket = nextBucket;
    }

    public int getParallel() {
        return parallel;
    }

    public void setParallel(int parallel) {
        this.parallel = parallel;
    }

    public int getLast() {
        return last;
    }

    public void setLast(int last) {
        this.last = last;
    }

    public int getNext() {
        return next;
    }

    public void setNext(int next) {
        this.next = next;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public boolean isSkip() {
        return skip;
    }

    public void setSkip(boolean skip) {
        this.skip = skip;
    }
}
