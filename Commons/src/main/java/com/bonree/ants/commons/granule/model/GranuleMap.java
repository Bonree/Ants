package com.bonree.ants.commons.granule.model;

import java.io.Serializable;
import java.util.Set;
import java.util.TreeMap;

/**
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018/12/12 11:06
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 粒度属性集合
 * *****************************************************************************
 */
public class GranuleMap implements Serializable {

    /**
     * 概述：粒度集合 <粒度, 粒度对象>
     */
    private TreeMap<Integer, Granule> graMap = new TreeMap<>();

    public void put(Integer currGra, Granule granule) {
        this.graMap.put(currGra, granule);
    }

    public Granule get(Integer currGra) {
        return this.graMap.get(currGra);
    }

    public int size() {
        return this.graMap.size();
    }

    public Set<Integer> keySet() {
        return this.graMap.keySet();
    }

    public boolean isEmpty() {
        return this.graMap.isEmpty();
    }

    /**
     * 概述：当前集合的第一个粒度的对象
     */
    public Granule firstValue() {
        return this.graMap.firstEntry().getValue();
    }

    /**
     * 概述：当前集合的最后一个粒度的对象
     */
    public Granule tailValue() {
        return this.graMap.lastEntry().getValue();
    }

    /**
     * 概述：当前集合的第一个粒度
     */
    public int firstGra() {
        return this.graMap.firstEntry().getValue().getGranule();
    }

    /**
     * 概述：当前集合的最后一个粒度
     */
    public int tailGra() {
        return this.graMap.lastEntry().getValue().getGranule();
    }

    public boolean isSkip(Integer granule) {
        if (this.graMap.containsKey(granule)) {
            return this.graMap.get(granule).isSkip();
        }
        return false;
    }

    public boolean containsKey(Integer granule) {
        return this.graMap.containsKey(granule);
    }

    /**
     * 概述：获取指定粒度的上一个粒度
     * @param currGra 粒度
     */
    public int lastGra(int currGra) {
        return this.graMap.get(currGra).getLast();
    }

    /**
     * 概述：获取指定粒度的下一个粒度
     * @param currGra 粒度
     */
    public int nextGra(int currGra) {
        return this.graMap.get(currGra).getNext();
    }

    public TreeMap<Integer, Granule> getGraMap() {
        return graMap;
    }
}
