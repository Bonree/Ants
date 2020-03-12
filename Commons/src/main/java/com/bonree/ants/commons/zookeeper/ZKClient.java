package com.bonree.ants.commons.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ZKClient {

    private static final Logger log = LoggerFactory.getLogger(ZKClient.class);

    private CuratorFramework curator;      // 连接zk的对象
    private String path;                   // 节点路径

    /**
     * 初始化连接对象
     * @param zkHosts zk地址
     */
    public ZKClient(String zkHosts) {
        this(zkHosts, "");
    }

    /**
     * 初始化连接对象
     * @param zkHosts zk地址
     * @param path 需要操作zk上的节点路径
     */
    private ZKClient(String zkHosts, String path) {
        this.path = path;
        // 重试次数
        int retryCount = 3;
        // 每次休眠的时间
        int retrySleepMs = 2 * 1000;
        RetryPolicy retryPolicy = new RetryNTimes(retryCount, retrySleepMs);
        // 会话超时时间
        int sessionTimeoutMs = 300 * 1000;
        // 连接超时时间
        int connectionTimeoutMs = 30 * 1000;
        curator = CuratorFrameworkFactory.newClient(zkHosts, sessionTimeoutMs, connectionTimeoutMs, retryPolicy);
        curator.start();
    }

    /**
     * 概述： 创建节点
     */
    public void crateNode() {
        crateNode(path, CreateMode.PERSISTENT, new byte[0]);
    }

    /**
     * 概述： 创建节点
     * @param path 节点路径
     */
    void crateNode(String path) {
        crateNode(path, CreateMode.PERSISTENT, new byte[0]);
    }
    
    /**
     * 概述： 创建节点
     * @param path 节点路径
     * @param mode 节点的类型(永久,临时,有序等...)
     */
    public void crateNode(String path, CreateMode mode) {
        crateNode(path, mode, new byte[0]);
    }

    /**
     * 概述： 创建节点
     * @param path 节点路径
     * @param mode 节点的类型(永久,临时,有序等...)
     * @param data 节点内容
     */
    private void crateNode(String path, CreateMode mode, byte[] data) {
        try {
            curator.create().creatingParentContainersIfNeeded().withMode(mode).forPath(path, data);
        } catch (Exception ex) {
            log.error("Create node error! path: {}, mode: {}", path, mode.name(), ex);
        }
    }

    /**
     * 概述：检测节点是否存在
     */
    public boolean exists() {
        return exists(path);
    }

    /**
     * 概述：检测节点是否存在
     * @param path 节点路径
     */
    public boolean exists(String path) {
        Stat stat = null;
        try {
            stat = curator.checkExists().forPath(path);
        } catch (Exception ex) {
            log.error("Check exists error! path: {}", path, ex);
        }
        return stat != null;
    }

    /**
     * 概述：获取子节点列表
     */
    public List<String> getChildren() {
        return getChildren(path);
    }

    /**
     * 概述：获取子节点列表
     * @param path 节点路径
     */
    private List<String> getChildren(String path) {
        try {
            return curator.getChildren().forPath(path);
        } catch (Exception ex) {
            log.error("Get children node error! path: {}", path, ex);
        }
        return null;
    }

    /**
     * 概述：获取节点内容
     */
    public byte[] getData() {
        return getData(path);
    }

    /**
     * 概述：获取节点内容
     * @param path 节点路径
     */
    public byte[] getData(String path) {
        try {
            return curator.getData().forPath(path);
        } catch (Exception ex) {
            log.error("Get data error! path: {}", path, ex);
        }
        return null;
    }

    /**
     * 概述：设置节点内容
     * @param data 内容
     */
    public void setData(byte[] data) {
        setData(path, data);
    }

    /**
     * 概述：设置节点内容
     * @param path 节点路径
     * @param data 内容
     */
    void setData(String path, byte[] data) {
        try {
            curator.setData().forPath(path, data);
        } catch (Exception ex) {
            log.error("Set data error! path: {}", path, ex);
        }
    }

    /**
     * 概述：获取连接对象
     */
    public CuratorFramework getCuratorFramework() {
        return curator;
    }
    
    /**
     * 概述：关闭连接
     *
     */
    public void close(){
        CloseableUtils.closeQuietly(curator);
    }

}
