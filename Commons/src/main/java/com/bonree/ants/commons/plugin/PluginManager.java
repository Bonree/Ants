package com.bonree.ants.commons.plugin;

import java.io.File;
import java.util.Objects;

import com.bonree.ants.commons.GlobalContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.ants.commons.config.ConfigManager;
import com.bonree.ants.commons.redis.JedisClient;
import com.bonree.ants.commons.utils.FileUtils;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年5月28日 下午2:00:52
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 插件管理
 * ****************************************************************************
 */
public abstract class PluginManager extends ConfigManager {

    private static final Logger log = LoggerFactory.getLogger(PluginManager.class);

    private String pluginPath;
    private String pluginName;
    private String pluginRedisKey;
    private String serviceImpl;

    /**
     * 初始化
     *
     * @param pluginPath     保存插件jar到本地的路径
     * @param pluginName     插件jar名称
     * @param pluginRedisKey 插件jar在redis上存储的key
     * @param serviceImpl    插件实现类完成路径名称
     */
    public PluginManager(String pluginPath, String pluginName, String pluginRedisKey, String serviceImpl) {
        this.pluginPath = GlobalContext.getConfig().getRootPath() + pluginPath;
        this.pluginName = pluginName;
        this.pluginRedisKey = pluginRedisKey;
        this.serviceImpl = serviceImpl;
    }

    /**
     * 概述：加载插件对象
     *
     * @param reload 是否重启初始插件对象
     */
    public <T> T load(boolean reload) throws Exception {
        initPluginJar(pluginPath, pluginName, pluginRedisKey);
        ClassLoader.instance.initClassLoader(pluginPath, pluginName, serviceImpl, reload);
        return getPlugin(serviceImpl);
    }

    /**
     * 概述：初始插件jar到本地存储
     *
     * @param pluginPath     保存插件jar到本地的路径
     * @param pluginName     插件jar名称
     * @param pluginRedisKey 插件jar在redis上存储的key
     * @throws Exception
     */
    private void initPluginJar(String pluginPath, String pluginName, String pluginRedisKey) throws Exception {
        if (!FileUtils.exists(pluginPath, pluginName)) {
            byte[] etlByte = JedisClient.getRedisByKey(pluginRedisKey);
            FileUtils.byte2File(etlByte, pluginPath, pluginName);
            log.info("Save plugin file success! file: {}{}, key: {}, length: {}", pluginPath, pluginName, pluginRedisKey, etlByte != null ? etlByte.length : 0);
        }

        File file = new File(pluginPath);
        File[] files = file.listFiles();
        if (files == null) {
            log.error("plugin file list is null: {}", pluginPath);
            return;
        }
        for (File ff : Objects.requireNonNull(files)) {
            String fileName = ff.getName();
            if (!fileName.equals(pluginName)) {
                if (ff.delete()) {
                    log.info("Delete plugin file success: {}{}", pluginPath, fileName);
                } else {
                    log.info("Delete plugin file failed: {}{}", pluginPath, fileName);
                }
            }
        }
    }

    /**
     * 概述：获取插件对象
     *
     * @param serviceImpl 插件实现类完成路径名称
     * @return 插件对象
     * @throws Exception
     */
    protected abstract <T> T getPlugin(String serviceImpl) throws Exception;

    /**
     * 概述：保存插件对象
     *
     * @param plugin
     * @throws Exception
     */
    public abstract <T> void savePlugin(T plugin) throws Exception;

}
