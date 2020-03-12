package com.bonree.ants.commons.zookeeper;

import com.bonree.ants.commons.GlobalContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public enum ZKCommons {

    instance;

    private static final Logger log = LoggerFactory.getLogger(ZKCommons.class);

    private ZKClient client;

    ZKCommons() {
        client = new ZKClient(GlobalContext.getConfig().getZookeeperCluster());
    }

    /**
     * 概述：发送数据到zk
     * @param path 路径
     * @param data 内容
     */
    public void setData(String path, byte[] data) {
        try {
            if (!client.exists(path)) {
                client.crateNode(path);
            }
            client.setData(path, data);
        } catch (Exception ex) {
            log.error("Set data to zk error! ", ex);
        }
    }

    /**
     * 概述：发送数据到zk
     * @param path 路径
     * @param data 内容
     */
    public void setData(String path, String data) {
        try {
            setData(path, data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception ex) {
            log.error("Set data to zk error! ", ex);
        }
    }

    /**
     * 概述：获取zk上指定路径的数据
     * @param path 获取数据的路径
     * @return
     */
    public byte[] getData(String path) {
        return client.getData(path);
    }
}
