package com.bonree.ants.commons.schema;

import com.bonree.ants.commons.Commons;
import com.bonree.ants.commons.enums.AggType;
import com.bonree.ants.commons.enums.BizType;
import com.bonree.ants.commons.enums.Delim;
import com.bonree.ants.commons.schema.model.Schema;
import com.bonree.ants.commons.schema.model.SchemaData;
import com.bonree.ants.commons.schema.model.SchemaFields;
import com.bonree.ants.commons.utils.DateFormatUtils;
import com.bonree.ants.commons.utils.XmlParseUtils;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.Map.Entry;

/**
 * *****************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date: 2018年5月28日 下午2:02:18
 * @Author: <a href=mailto:zhangnl@bonree.com>张念礼</a>
 * @Description: Schema配置处理
 * ****************************************************************************
 */
public class SchemaConfigImpl extends SchemaManager {

    private static final Logger log = LoggerFactory.getLogger(SchemaConfigImpl.class);

    /**
     * Schema配置信息对象
     */
    private Schema schema = new Schema();

    public SchemaConfigImpl() {
    }

    public SchemaConfigImpl(Schema schema) {
        this.schema = schema;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    /**
     * 加载业务描述信息
     */
    @Override
    public void loadDesc(XmlParseUtils config) throws Exception {
        Element descElement = config.getElement("data-desc");
        schema.setType(config.getAttr(descElement, "type"));
        Elements relations = descElement.children();
        for (Element ele : relations) {
            String path = config.getAttr(ele, "path");
            String text = config.getText(ele);
            schema.putRelation(text, path);
        }
    }


    @Override
    public void loadBiz(String configPath) throws Exception {
        // 1.加载正常业务schema信息
        loadSchemaFile(configPath + SchemaGLobals.SCHEMA_DIR, BizType.NORMAL.type());
        // 2.加载基线schema信息
        loadSchemaFile(configPath + SchemaGLobals.EXTENTION_DIR, BizType.BASELINE.type());
        // 3.加载活跃数schema信息
        loadSchemaFile(configPath + SchemaGLobals.ACTIVE_DIR, BizType.ACTIVE.type());
        long currTime = DateFormatUtils.getDay(new Date()).getTime();
        schema.setCreateTime(currTime);
    }

    /**
     * 概述：加载业务配置信息
     *
     * @param configPath Schema配置文件目录
     * @param type       业务类型,用于区分正常业务或基线业务
     * @throws Exception
     */
    private void loadSchemaFile(String configPath, String type) throws Exception {
        File file = new File(configPath);
        File[] files = file.listFiles();
        if (files == null || files.length <= 0) {
            log.warn("Schema file dir is not exists or is empty! path: {}", configPath);
            return;
        }
        XmlParseUtils schemaConfig;
        for (File itemFile : files) {
            schemaConfig = new XmlParseUtils(itemFile);
            loadSchema(schemaConfig, type);
        }
    }

    /**
     * 加载业务配置信息
     *
     * @param schemaConfig schema配置文件
     * @param type         业务类型
     * @throws Exception
     */
    private void loadSchema(XmlParseUtils schemaConfig, String type) throws Exception {
        Element schemaElement = schemaConfig.getElement("schema");
        Elements tables = schemaElement.children();
        Elements fieldEle;
        SchemaFields fields;
        SchemaData data;
        Map<String, SchemaData> tempMap = new HashMap<>();
        for (Element ele : tables) {
            String tableName = schemaConfig.getAttr(ele, "name");
            String isGranule = schemaConfig.getAttr(ele, "granule");
            String part = schemaConfig.getAttr(ele, "part");
            if (StringUtils.isNotEmpty(part)) {
                tableName = tableName + Delim.SIGN.value() + part;
            }
            if (!tempMap.containsKey(tableName)) {
                tempMap.put(tableName, new SchemaData());
            }
            data = tempMap.get(tableName);
            data.setBizName(tableName);
            if (StringUtils.isNotEmpty(part)) {
                data.setPart(Integer.parseInt(part));
            }
            if (StringUtils.isNotEmpty(isGranule)) {
                data.setGranule("true".equals(isGranule));
            }
            data.setBizType(type);
            fieldEle = ele.children();
            for (Element field : fieldEle) {
                fields = new SchemaFields();
                fields.setName(schemaConfig.getAttr(field, "name"));
                fields.setSource(schemaConfig.getAttr(field, "source"));
                fields.setType(schemaConfig.getAttr(field, "type").toLowerCase());
                fields.setValueType(schemaConfig.getAttr(field, "valueType").toUpperCase());
                String expr = schemaConfig.getAttr(field, "expr").replace("()", "");
                fields.setExpr(expr);
                String[] funArr = expr.split("\\$");
                if (funArr.length == 2) {
                    fields.setExpressList(getExpressFields(funArr[1]));
                }
                // 只有基线类型业务时,对中位数属性增加count和sum两个辅助属性
                if (BizType.BASELINE.type().equals(type) && funArr[0].startsWith(AggType.MEDIAN.type())) {
                    SchemaFields fields1 = Commons.clone(fields);
                    fields1.setExpr("sum$(1)");
                    fields1.setExpressList(Collections.singletonList("1"));
                    fields1.setName(fields1.getName().concat("_count"));
                    data.addFields(fields1);
                    SchemaFields fields2 = Commons.clone(fields);
                    String tmpExpr = fields2.getExpr();
                    int index = tmpExpr.indexOf("$");
                    if (index > -1) {
                        fields2.setExpr("sum".concat(expr.substring(index)));
                    } else {
                        fields2.setExpr("sum");
                    }
                    fields2.setName(fields2.getName().concat("_sum"));
                    data.addFields(fields2);
                }
                data.addFields(fields);
            }
            schema.putData(data);
        }
    }

    /**
     * 概述：获取自定义函数表达式中的关联字段
     *
     * @param expressStr 字符串表达式
     * @return 表达式集合
     */
    private static List<String> getExpressFields(String expressStr) {
        List<String> expressList = new ArrayList<>();
        final int sz = expressStr.length();
        StringBuilder sb = null;
        for (int i = 0; i < sz; i++) {
            char chars = expressStr.charAt(i);
            if (chars == ' ') {
                continue;
            }
            if (sb == null) {
                sb = new StringBuilder();
            }
            if (chars == '>' || chars == '<' || chars == '&' || chars == '|' || chars == '=' || chars == '+' || chars == '-' || chars == '*' || chars == '/' || chars == '(' || chars == ')') {
                if (sb.length() > 0) {
                    expressList.add(sb.toString());
                }
                sb = null;
            } else {
                sb.append(chars);
            }
        }
        if (sb != null) {
            expressList.add(sb.toString());
        }
        return expressList;
    }
    
    /**
     * 概述：初始入库sql
     */
    @Override
    public void initDbSql() {
        StringBuilder sql;
        SchemaData data;
        for (Entry<String, SchemaData> entry : schema.getData().entrySet()) {
            data = entry.getValue();
            if (BizType.BASELINE.type().equals(data.getBizType()) || BizType.ALERT.type().equals(data.getBizType())) {
                continue; // 排除不需要入库的业务
            }
            sql = new StringBuilder();
            sql.append("INSERT INTO ");
            sql.append("%s");
            StringBuilder fieldStr = null;
            StringBuilder valueStr = null;
            for (SchemaFields field : data.getFieldsList()) {
                if (SchemaGLobals.HIDDEN.equals(field.getType())) {
                    continue; // 隐藏属性不入库
                }
                if (fieldStr == null) {
                    fieldStr = new StringBuilder(" (monitor_time,");
                    valueStr = new StringBuilder(") VALUES (?,");
                    if (data.getPart() > 0) {
                        fieldStr.append("type,");
                        valueStr.append("?,");
                    }
                } else {
                    fieldStr.append(",");
                    valueStr.append(",");
                }
                fieldStr.append(field.getName());
                valueStr.append("?");
            }
            Objects.requireNonNull(valueStr).append(")");
            sql.append(fieldStr);
            sql.append(valueStr);
            data.setBizSql(sql.toString());
        }
    }

    /**
     * 保存schema对象信息
     */
    @Override
    public void saveSchema() throws Exception {
        save(SchemaGLobals.SCHEMA_QUEUE, schema);
    }
}
