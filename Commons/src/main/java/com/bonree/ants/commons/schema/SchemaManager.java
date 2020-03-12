package com.bonree.ants.commons.schema;

import com.bonree.ants.commons.config.ConfigManager;
import com.bonree.ants.commons.utils.XmlParseUtils;
import com.bonree.ants.commons.schema.model.Schema;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @Date: 2018年5月28日 下午2:01:40
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: Schema配置管理
 *****************************************************************************
 */
public abstract class SchemaManager extends ConfigManager {

    public abstract Schema getSchema();

    /**
     * 概述：加载各业务表配置
     * @param configPath schema配置文件目录
     * @throws Exception
     */
    public abstract void loadBiz(String configPath) throws Exception;
    
    /**
     * 概述：加载schema业务表描述信息
     * @param config schema.xml文件对象
     * @throws Exception
     */
    public abstract void loadDesc(XmlParseUtils config) throws Exception;

    /** 
     * 概述：初始入库sql
     * @throws Exception
     */
    public abstract void initDbSql() throws Exception;
    
    /**
     * 概述：保存Schema信息
     * @throws Exception
     */
    public abstract void saveSchema() throws Exception;
}
