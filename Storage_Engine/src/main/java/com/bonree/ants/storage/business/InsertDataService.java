package com.bonree.ants.storage.business;

import com.bonree.ants.commons.GlobalContext;
import com.bonree.ants.commons.enums.DataType;
import com.bonree.ants.commons.enums.Delim;
import com.bonree.ants.commons.schema.SchemaGLobals;
import com.bonree.ants.commons.schema.model.SchemaData;
import com.bonree.ants.commons.schema.model.SchemaFields;
import com.bonree.ants.plugin.etl.model.CustomData;
import com.bonree.ants.plugin.etl.model.Records;
import com.bonree.ants.plugin.storage.IStorageService;
import com.bonree.ants.plugin.storage.model.DataResult;
import com.bonree.ants.storage.utils.DBUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年4月17日 下午3:07:54
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: 入库操作
 * ****************************************************************************
 */
public class InsertDataService implements IStorageService {

    private static final Logger log = LoggerFactory.getLogger(InsertDataService.class);

    private SchemaData schema;  // schema配置信息
    private long time;          // 数据粒度时间

    public InsertDataService(SchemaData schema, long time) {
        this.schema = schema;
        this.time = time;
    }

    @Override
    public void message(DataResult content) throws Exception {
        if (schema == null) {
            log.warn("Insert data not found Schema info!!! bizName: {}", content.getBizName());
            return;
        }
        String bizKey = content.getTime() + "_" + content.getBizName() + "_" + content.getGranule();
        List<Records<String, Object>> recordsList = content.get();
        if (recordsList.isEmpty()) {
            log.info("Result data is empty! " + bizKey);
            return;
        }
        Connection conn;
        try {
            // 获取数据库连接
            conn = DBUtils.getConnection(false);
        } catch (SQLException ex) {
            log.error("Connetion error!" + bizKey, ex);
            throw ex;
        }
        int count = recordsList.size();
        PreparedStatement stat = null;
        String bizSql = null;
        try {
            bizSql = buildSql(content.getGranule(), schema);
            // 数据批量入库
            stat = conn.prepareStatement(bizSql.toUpperCase());
            for (Records<String, Object> records : recordsList) {
                int num = 1;
                setStatData(num, DataType.TIMESTAMP.name(), time, stat);
                if (schema.getPart() > 0) {
                    setStatData(++num, DataType.LONG.name(), schema.getPart(), stat);
                }
                for (SchemaFields fields : schema.getFieldsList()) { // 处理每一行
                    if (SchemaGLobals.HIDDEN.equals(fields.getType())) {
                        continue; // 辅助属性不入库
                    }
                    Object value = "";
                    if (records.containsKey(fields.getName())) {
                        value = records.get(fields.getName());
                    }
                    setStatData(++num, fields.getValueType(), value, stat);
                }
                stat.addBatch();
            }
            stat.executeBatch();
            conn.commit();
            log.info("Insert data success! {}, count: {}", bizKey, count);
        } catch (Exception ex) {
            try {
                conn.rollback();
            } catch (SQLException e) {
                log.warn("Roll back error, {} ", bizKey, e);
            }
            log.error("Insert data error! biz: {}, count: {}, bizSql: {}, record: {}", bizKey, count, bizSql, recordsList.get(0), ex);
            throw new Exception(ex);
        } finally {
            DBUtils.close(null, stat, conn);
        }
    }

    /**
     * 概述：生成入库sql
     *
     * @param granule 当前粒度
     * @param schema  schema配置信息
     */
    private static String buildSql(int granule, SchemaData schema) {
        String tableName = schema.getBizName();
        int index = tableName.lastIndexOf(Delim.SIGN.value());
        if (index != -1) {
            tableName = tableName.substring(0, index);
        }
        tableName = "t_" + tableName;
        String suffix = getTableSuffix(granule);
        String bizSql = schema.getBizSql();
        return String.format(bizSql, tableName + suffix);
    }

    /**
     * 概述：往PreparedStatement对象中赋值
     *
     * @param num  序号,由1开始
     * @param type 字段类型
     * @param data 数据内容
     * @param stat PreparedStatement对象
     */
    private static void setStatData(int num, String type, Object data, PreparedStatement stat) throws Exception {
        if (data == null || "".equals(data)) {
            if (type.equals(DataType.DOUBLE.name())) {
                data = 0.0;
            } else if (type.equals(DataType.STRING.name())) {
                data = "";
            } else {
                data = 0L;
            }
        }
        try {
            if (type.equals(DataType.LONG.name())) {
                stat.setLong(num, ((Number) data).longValue());
            } else if (type.equals(DataType.DOUBLE.name())) {
                stat.setDouble(num, ((Number) data).doubleValue());
            } else if (type.equals(DataType.STRING.name())) {
                stat.setString(num, (String) data);
            } else if (type.equals(DataType.TIMESTAMP.name())) {
                stat.setTimestamp(num, new Timestamp(((Number) data).longValue()));
            } else {
                throw new IllegalArgumentException("Illegal character type: " + type);
            }
        } catch (Exception ex) {
            log.error("PreparedStatement set data error! num: {}, type:{}, data:{}", num, type, data);
            throw ex;
        }
    }

    /**
     * 概述：根据时间粒度获取数据表名的后缀
     *
     * @param granule 时间粒度
     */
    private static String getTableSuffix(int granule) {
        if (granule == 0) {
            return "";
        }
        if (granule >= 60 && granule < 3600) {
            return "_min" + granule / 60;
        }
        if (granule >= 3600 && granule < 86400) {
            return "_hour" + (granule / 60 / 60);
        }
        if (granule == 86400) {
            return "_day1";
        }
        return "_month1";
    }

    @Override
    public void message(CustomData customData) {

    }
}
