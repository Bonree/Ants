package com.bonree.ants.commons.utils;

import java.io.Serializable;
import java.util.concurrent.CopyOnWriteArrayList;
/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @Date: 2018年5月2日 下午4:30:41
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 固定大小队列
 *****************************************************************************
 */
public class LimitQueue<T> implements Serializable {
    
    private static final long serialVersionUID = 1L;

    private int limit = Integer.MAX_VALUE; // 队列长度
    
    private CopyOnWriteArrayList<T> queue = new CopyOnWriteArrayList<>();
    
    public LimitQueue(){
    }
    
    public LimitQueue(int limit) {
        this.limit = limit;
    }

    /** 
     * 入队
     */
    public synchronized void offer(T t) {
        if (queue.size() >= limit) {
            queue.remove(0);  //如果超出长度,入队时,先出队  
        }
        queue.add(t);
    }

    public CopyOnWriteArrayList<T> getQueue() {
        return queue;
    }
    
    public T peek() {  
        if (!queue.isEmpty()) {
            return queue.get(0);  
        }
        return null;
    } 
    
    public void del(int i){
        queue.remove(i);
    }
    
    public int size() {  
        return queue.size();  
    }
    
    public int limit(){
        return limit;
    }
}
