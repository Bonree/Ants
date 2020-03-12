package com.bonree.ants.plugin.etl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bonree.ants.plugin.etl.model.CustomData;
import com.bonree.ants.plugin.etl.model.DataSet;
import com.bonree.ants.plugin.etl.model.Records;
import com.bonree.ants.plugin.etl.model.SchemaConfig;
import com.bonree.ants.plugin.etl.model.SchemaConfig.Field;
import com.bonree.ants.plugin.etl.utils.XpathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class RichEtlService implements IEtlService {

    protected static final Logger LOG = LoggerFactory.getLogger("etl");
    private static final String XML_TYPE = "xml";
    protected String ROOT = "/ants/";

    @Override
    public void init(Object obj) throws Exception {

    }

    @Override
    public DataSet onMessage(byte[] content, SchemaConfig config) throws Exception {
        if (content == null || content.length == 0) {
            LOG.warn("Received content is null!");
            return null;
        }
        Object dataObj = convertData(config.getType(), content);
        long time = getMonitorTime(dataObj);
        // 1.初始化关联信息
        Map<String, String> relationMap = config.getRelation();
        Map<String, Object> valueMap = new HashMap<>();
        String source;// 业务属性名称(业务表xml中属性"source"的值)
        String path;  // 属性值对应的路径
        Object value; // 解析出的属性值
        for (Entry<String, String> entry : relationMap.entrySet()) {
            source = entry.getKey();
            path = entry.getValue();
            try {
                value = getDataValue(path, dataObj);
                value = deal(source, value);
                if (value == null) {
                    LOG.info("Parse json value is null ! soruce: {}, path: {}", source, path);
                    continue;
                }
            } catch (Exception ex) {
                LOG.error("Parse json value error! soruce: {}, path: {}", source, path, ex);
                continue;
            }
            valueMap.put(source, value);
        }
        // 2.根据关联信息,对业务数据赋初值
        DataSet data = new DataSet();
        Map<String, List<Field>> bizMap = config.getConfig();
        List<Field> fieldList;// 业务表中字段对象集合
        String tableName;// 业务表名称
        Records<String, Object> record; // 一行记录
        String name;    // 字段名称
        CustomData customData = null;
        for (Entry<String, List<Field>> entry : bizMap.entrySet()) { // 遍历每个业务表
            tableName = entry.getKey();
            fieldList = entry.getValue();
            record = new Records<>();
            record.put("monitorTime", time); // 内置字段
            for (Field field : fieldList) {
                name = field.getName();
                source = field.getSource();
                value = valueMap.get(source);
                record.put(name, value);
            }
            try {
                record = deal(tableName, record); // 字段处理
            } catch (Exception ex) {
                LOG.error("Parse field error! tableName: {}, record: {}, value: {}", tableName, record);
            }
            if (!isValidate(tableName, record)) { // 数据过滤验证
                continue;
            }
            CustomData.File file = snapshot(tableName, record); // 快照处理
            if (file != null) {
                if (customData == null) {
                    customData = new CustomData();
                }
                customData.addData(file); // 保存每条快照文件
            }
            data.addRecords(tableName, record); // 保存每条数据
        }
        if (customData != null && !customData.getDataList().isEmpty()) {
            data.setCustomData(customData); // 保存快照对象
        }
        return data;
    }

    @Override
    public void close() throws Exception {

    }

    /**
     * 概述：根据数据格式类型解析数据
     *
     * @param type    数据格式类型
     * @param content 数据内容
     */
    protected Object convertData(String type, byte[] content) throws Exception {
        String strData = new String(content, StandardCharsets.UTF_8);
        if (XML_TYPE.equals(type)) {
            return XpathUtils.instance.createDoc(new ByteArrayInputStream(content));
        } else {
            JSONObject jsonObj = JSON.parseObject(strData);
            if (!jsonObj.containsKey("data")) {
                LOG.warn("Attribute:'data' is null! data: s{}", strData);
                return null;
            }
            return jsonObj;
        }
    }

    /**
     * 概述：获取数据时间
     *
     * @param data 解析后的数据内容
     */
    protected long getMonitorTime(Object data) throws Exception {
        if (data instanceof Document) {
            return XpathUtils.instance.getNumberText((Document) data, ROOT.concat("monitorTime")).longValue();
        } else {
            return ((JSONObject) data).getLongValue("monitorTime");
        }
    }

    /**
     * 概述：获取数据时间
     *
     * @param path 数路径
     * @param data 数据内容
     */
    private Object getDataValue(String path, Object data) throws Exception {
        if (data instanceof Document) {
            return getXmlValue(path, (Document) data);
        } else {
            return getJosnValue(path, (JSONObject) data);
        }
    }

    /**
     * 概述：获取json路径中的值
     *
     * @param path    json路径
     * @param jsonObj json内容
     */
    protected Object getJosnValue(String path, JSONObject jsonObj) {
        String[] pathArr = path.split("\\.");
        JSONObject tmpObj = jsonObj;
        for (int i = 0; i < pathArr.length; i++) {
            String key = pathArr[i];
            if (tmpObj == null) {
                return null;
            }
            if (i < pathArr.length - 1) {
                tmpObj = tmpObj.getJSONObject(key);
            } else {
                return tmpObj.get(key);
            }
        }
        return null;
    }

    /**
     * 概述：获取xml路径中的值
     *
     * @param path xml路径
     * @param doc  xml内容
     */
    protected Object getXmlValue(String path, Document doc) throws Exception {
        String value = XpathUtils.instance.getStringText(doc, path);
        if (isNumeric(value)) {
            return NumberFormat.getInstance().parse(value);
        }
        return value;
    }

    /**
     * 概述：此方法主要针对一些公共字段的数据处理(需要特殊处理,可重写此方法)
     *
     * @param source 属性名称(业务表xml中属性"source"的值)
     * @param value  字段值
     * @return 处理后的新值
     */
    protected Object deal(String source, Object value) {
        return value; // 默认数据不做任何处理,原值返回.
    }

    /**
     * 概述：业务表中赋完初值的一行数据的处理(需要特殊处理,可重写此方法)
     *
     * @param tableName 数据表名称
     * @param record    一行数据
     * @return 处理后的新值
     */
    protected Records<String, Object> deal(String tableName, Records<String, Object> record) {
        return record; // 默认数据不做任何处理,原值返回.
    }

    /**
     * 概述：数据验证(需要特殊处理,可重写此方法)
     *
     * @param tableName 数据表名称
     * @param record    一行数据
     * @return true 保留; false 不保留
     */
    protected boolean isValidate(String tableName, Records<String, Object> record) {
        return true; // 默认全部保留
    }

    /**
     * 概述：数据快照处理(需要重写此方法)
     *
     * @param tableName 数据表名称
     * @param record    一行数据
     * @return 一条快照文件对象
     */
    protected CustomData.File snapshot(String tableName, Records<String, Object> record) {
        return null; // 默认不保留快照
    }

    /**
     * 概述：判断字符串是否为数字
     *
     * @param cs 字符串
     * @return true 数字, false 不为数字
     */
    public static boolean isNumeric(final String cs) {
        if (cs == null || "".equals(cs)) {
            return false;
        }
        int n = 0;
        final int sz = cs.length();
        for (int i = 0; i < sz; i++) {
            char chars = cs.charAt(i);
            if (chars == '.') {
                n++;
            }
            if (n >= 2) {
                return false;
            }
            if (!Character.isDigit(chars) && chars != '.') {
                return false;
            }
        }
        return true;
    }
}
