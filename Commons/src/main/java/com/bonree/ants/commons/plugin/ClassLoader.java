package com.bonree.ants.commons.plugin;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月20日 下午6:07:39
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 类加载器
 *****************************************************************************
 */
public enum ClassLoader {

    instance;

    private final Logger log = LoggerFactory.getLogger(ClassLoader.class);

    private Map<String, URLClassLoader> loaderMap = new HashMap<>();

    /**
     * 概述：初始加载ClassLoader
     *
     */
    public void initClassLoader(String pluginPath, String pluginName, String className, boolean reload) {
        try {
            if (!reload) {
                if (loaderMap.containsKey(className)){
                    return; // 没有重新加载时,如果已经存储该实例,则直接退出
                }
            }
            String filePath = pluginPath + File.separator + pluginName;
            File file = new File(filePath);
            if (!file.exists()) {
                log.warn("plugin jar not exists : {}", filePath);
            }
            URL[] urls = new URL[] { file.toURI().toURL() };
            URLClassLoader loader = URLClassLoader.newInstance(urls, Thread.currentThread().getContextClassLoader());
            loaderMap.put(className, loader);
            log.info("Init class loader success! path: {}", filePath);
        } catch (Exception ex) {
            log.error("ClassLoader instance error! ", ex);
        }
    }

    /**
     * 概述：获取插件对象
     * @param className 实现类的类名称(完整包路径)
     * @param required 实际要转换成的父类对象
     * @return
     */
    @SuppressWarnings("unchecked")
    public <T> T getPlugin(String className, Class<T> required) {
        Class<?> cls;
        try {
            cls = loaderMap.get(className).loadClass(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("can not find class:" + className, e);
        } catch (Exception e) {
            throw new IllegalArgumentException("get plugin error! class:" + className, e);
        }
        if (required.isAssignableFrom(cls)) {
            try {
                return (T) cls.newInstance();
            } catch (Exception e) {
                throw new IllegalArgumentException("can not newInstance class:" + className, e);
            }
        }
        throw new IllegalArgumentException("class:" + className + " not sub class of " + required);
    }
}
