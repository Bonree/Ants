package com.bonree.ants.commons.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class NodeListener {

    private static final Logger log = LoggerFactory.getLogger(NodeListener.class);

    private NodeCache nodeCache;    // 节点监听
    private volatile String data;   // 节点数据内容

    /**
     * 初始化节点监听对象
     * @param path  监听的路径
     * @param curator 连接对象
     */
    public NodeListener(String path, CuratorFramework curator) {
        try {
            this.nodeCache = new NodeCache(curator, path, false);
            nodeCache.start();
        } catch (Exception ex) {
            log.error("Start node cache error! path: {}", path, ex);
        }
    }
        
    /**
     * 概述：获取监听节点内容
     * @return
     */
    public String getData() {
        return data;
    }
    
    /**
     * 概述：清空监听到的内容
     */
    public void clear() {
        this.data = "";
    }

    /**
     * 概述：添加节点监听
     */
    public void addNodeListener() {
        NodeCacheListener listener = new NodeCacheListener() {

            @Override
            public void nodeChanged() throws Exception {
                ChildData childData = nodeCache.getCurrentData();
                if (childData != null) {
                    data = new String(childData.getData(), StandardCharsets.UTF_8);
                }
            }
        };
        nodeCache.getListenable().addListener(listener);
    }

    /**
     * 概述：关闭连接
     *
     */
    public void close() {
        CloseableUtils.closeQuietly(nodeCache);
    }
}
