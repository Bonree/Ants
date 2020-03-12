package com.bonree.ants.storage.utils;

import com.alibaba.fastjson.JSON;
import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.config.AntsConfig;
import com.bonree.ants.commons.utils.XmlParseUtils;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import security.impl.EncryptionAlgorithmImpl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司 Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights
 * Reserved.
 *
 * @Date: May 17, 2016 10:33:30 AM
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: Version: 1.0
 ******************************************************************************/
public class DBUtils {

    private static final Logger log = LoggerFactory.getLogger(DBUtils.class);

    /**
     * 连接池
     */
    private static ComboPooledDataSource comboPool = null;

    /**
     * 概述：加载db连接池参数
     *
     * @param config 配置文件对象
     */
    public static void initDbParams(XmlParseUtils config) throws Exception {
        // 加载db配置信息
        Map<String, String> DB_PARAMS = new HashMap<>();
        Element dbElement = config.getElement("db");
        DB_PARAMS.put(AntsConfig.DB_DRIVER, config.getStringValue(dbElement, "driver.class"));
        DB_PARAMS.put(AntsConfig.DB_URL, config.getStringValue(dbElement, "url"));
        DB_PARAMS.put(AntsConfig.DB_USER, config.getStringValue(dbElement, "username"));
        String password = new EncryptionAlgorithmImpl().getDecryptMessage(config.getStringValue(dbElement, "password"));
        DB_PARAMS.put(AntsConfig.DB_PSWD, password);

        // 加载c3po连接池信息
        Element poolElement = config.getElement("pool");
        DB_PARAMS.put(AntsConfig.DB_CHECKOUT_TIME, config.getStringValue(poolElement, "checkout.timeout"));
        DB_PARAMS.put(AntsConfig.DB_MAX_STATEMENTS_PER_CONNECTION, config.getStringValue(poolElement, "max.statements.per.connection"));
        DB_PARAMS.put(AntsConfig.DB_INITIAL_POOL_SIZE, config.getStringValue(poolElement, "initial.pool.size"));
        DB_PARAMS.put(AntsConfig.DB_MAX_IDLE_TIME, config.getStringValue(poolElement, "max.idle.time"));
        DB_PARAMS.put(AntsConfig.DB_MAX_POOL_SIZE, config.getStringValue(poolElement, "max.pool.size"));
        DB_PARAMS.put(AntsConfig.DB_MIN_POOL_SIZE, config.getStringValue(poolElement, "min.pool.size"));
        DB_PARAMS.put(AntsConfig.DB_ACQUIRE_INCREMENT, config.getStringValue(poolElement, "acquire.increment"));
        DB_PARAMS.put(AntsConfig.DB_IDLE_CONNECTION_TEST_PERIOD, config.getStringValue(poolElement, "idle.connection.test.period"));
        DB_PARAMS.put(AntsConfig.DB_NUM_HELPER_THREADS, config.getStringValue(poolElement, "num.helper.threads"));
        DB_PARAMS.put(AntsConfig.DB_PREFERRED_TEST_QUERY, config.getStringValue(poolElement, "preferred.test.query"));
        DB_PARAMS.put(AntsConfig.DB_STATEMENT_CACHE_NUM_DEFERRED_CLOSE_THREADS, config.getStringValue(poolElement, "statement.cache.num.deferred.close.threads"));
        DB_PARAMS.put(AntsConfig.DB_UNRETURNED_CONNECTION_TIMEOUT, config.getStringValue(poolElement, "unreturned.connection.timeout"));
        GlobalContext.getConfig().setDbParams(JSON.toJSONString(DB_PARAMS));
    }

    /**
     * 概述：初始化DB连接池, 调用此方法前需要先初始参数:DB_PARAMS
     */
    public static void init() {
        try {
            Map<String, String> DB_PARAMS = JSON.parseObject(GlobalContext.getConfig().getDbParams(), Map.class);
            String user = DB_PARAMS.get(AntsConfig.DB_USER);
            String password = DB_PARAMS.get(AntsConfig.DB_PSWD);
            String jdbcUrl = DB_PARAMS.get(AntsConfig.DB_URL);
            ComboPooledDataSource comboPooledDataSource = new ComboPooledDataSource();
            comboPooledDataSource.setJdbcUrl(jdbcUrl);
            comboPooledDataSource.setUser(user);
            comboPooledDataSource.setPassword(password);
            comboPooledDataSource.setDriverClass(DB_PARAMS.get(AntsConfig.DB_DRIVER));
            comboPooledDataSource.setMaxIdleTime(Integer.parseInt(DB_PARAMS.get(AntsConfig.DB_MAX_IDLE_TIME)));
            comboPooledDataSource.setCheckoutTimeout(Integer.parseInt(DB_PARAMS.get(AntsConfig.DB_CHECKOUT_TIME)));
            comboPooledDataSource.setMaxStatementsPerConnection(Integer.parseInt(DB_PARAMS.get(AntsConfig.DB_MAX_STATEMENTS_PER_CONNECTION)));
            comboPooledDataSource.setMaxPoolSize(Integer.parseInt(DB_PARAMS.get(AntsConfig.DB_MAX_POOL_SIZE)));
            comboPooledDataSource.setMinPoolSize(Integer.parseInt(DB_PARAMS.get(AntsConfig.DB_MIN_POOL_SIZE)));
            comboPooledDataSource.setAcquireIncrement(Integer.parseInt(DB_PARAMS.get(AntsConfig.DB_ACQUIRE_INCREMENT)));
            comboPooledDataSource.setIdleConnectionTestPeriod(Integer.parseInt(DB_PARAMS.get(AntsConfig.DB_IDLE_CONNECTION_TEST_PERIOD)));
            comboPooledDataSource.setNumHelperThreads(Integer.parseInt(DB_PARAMS.get(AntsConfig.DB_NUM_HELPER_THREADS)));
            comboPooledDataSource.setPreferredTestQuery(DB_PARAMS.get(AntsConfig.DB_PREFERRED_TEST_QUERY));
            comboPooledDataSource.setStatementCacheNumDeferredCloseThreads(Integer.parseInt(DB_PARAMS.get(AntsConfig.DB_STATEMENT_CACHE_NUM_DEFERRED_CLOSE_THREADS)));
            comboPooledDataSource.setUnreturnedConnectionTimeout(Integer.parseInt(DB_PARAMS.get(AntsConfig.DB_UNRETURNED_CONNECTION_TIMEOUT)));
            // 业务需要随时知道数据连接是否可用，性能方面，因为每次都是批量处理，获取次数不会太频繁
            comboPooledDataSource.setTestConnectionOnCheckout(true);
            comboPool = comboPooledDataSource;
            log.info("Init combopool complete....");
        } catch (Exception e) {
            log.error("Init combopool error!", e);
            System.exit(0);
        }
    }

    /**
     * 概述：获取数据库连接
     *
     * @param autoCommit 是否自动提交,true:自动提交,false:手动提交
     * @return 数据库连接对象
     */
    public static Connection getConnection(boolean autoCommit) throws SQLException {
        Connection conn;
        try {
            conn = comboPool.getConnection();
            conn.setAutoCommit(autoCommit);
        } catch (SQLException ex) {
            log.error("Get db connection error! ", ex);
            throw ex;
        }
        return conn;
    }

    /**
     * 关闭Connection
     */
    public static void close(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭Statement
     */
    public static void close(Statement stat) {
        try {
            if (stat != null) {
                stat.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭ResultSet
     */
    public static void close(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭连接
     */
    public static void close(ResultSet rs, Statement stmt, Connection conn) {
        close(rs);
        close(stmt);
        close(conn);
    }
}
